/* Copyright (c) 2010 Tim Deegan <Tim.Deegan@citrix.com> 
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE. */

/* Stream filesysytem overlay.
 *
 * A streamfs filesystem is a backend onto a directory of a real filesystem, 
 * passing through exactly the same contents, except that: 
 * 
 * No symlinks, no hard links, no device nodes.
 * Reads past the end of a file that's "streaming" will block until the 
 * relevant data appears or the file is marked as no longer streaming. 
 *
 * fstab runes (for mounting /stream as an overlay of /tmp): 
 * streamfs#/tmp /stream fuse user,noauto,direct_io 0 0
 *
 * To turn a file into a stream from python:
 * fcntl.ioctl(open(sys.argv[1], "a+"), 21256)
 *
 * To close it when the writing is done
 * fcntl.ioctl(open(sys.argv[1], "a+"), 21255)
 */

#include <sys/select.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/types.h>
#include <alloca.h>
#include <dirent.h>
#include <errno.h>
#include <malloc.h>
#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#define FUSE_USE_VERSION 26
#include <fuse.h>

/* XXX pick a better IOCTL?  These are CDROM ones, and take no arguments */ 
#define OPEN_STREAM_IOCTL 0x00005308
#define CLOSE_STREAM_IOCTL 0x00005307

struct stream {
    char *path;
    int streaming;
    unsigned int ref;
    struct stream *next;
};

struct fh {
    struct stream *sp;
    DIR *dirp; /* If it's a directory */
    int fd;
};

/* Retrieve the base path of the backing directory */
static const char *basepath;

/* Linked list of all open or interesting streams */
static struct stream *streams;
pthread_mutex_t streams_mutex;

/* Refcount streams */
static void put_stream(struct stream *s)
{
    struct stream *p;
    pthread_mutex_lock(&streams_mutex);
    s->ref--;
    if (s->ref != 0) {
        pthread_mutex_unlock(&streams_mutex);
        return;
    }
    if (s == streams)
        streams = s->next;
    else {
        for (p = streams; p; p = s->next) {
            if (p->next == s) {
                p->next = s->next;
                break;
            }
        }
    }
    pthread_mutex_unlock(&streams_mutex);
    free(s->path);
    free(s);
} 

/* Find a stream with the given path */
static struct stream *find_stream(const char *path) 
{
    struct stream *s;

    pthread_mutex_lock(&streams_mutex);
    for (s = streams; s; s = s->next) {
        if (!strcmp(s->path, path)) {
            s->ref++;
            pthread_mutex_unlock(&streams_mutex);
            return s;
        }
    }
    
    if (!(s = calloc(1, sizeof *s))) {
        pthread_mutex_unlock(&streams_mutex);
        return NULL;
    }
    s->path = strdup(path);
    if (!s->path) {
        free(s);
        pthread_mutex_unlock(&streams_mutex);
        return NULL;
    }
    s->ref = 1;

    s->next = streams; 
    streams = s;
    pthread_mutex_unlock(&streams_mutex);

    return s;
}

/* Build (on the stack!) a concatenation of base path and the given path */
/* XXX TODO sanity check path lengths */
#define fixpath(p) ({                                   \
    const char *base = basepath;                        \
    char *buf = alloca(strlen(base) + strlen(p));       \
    strcpy(buf, base);                                  \
    strcat(buf, p);                                     \
    buf;                                                \
})

/* Extract our per-file state from the FUSE struct */
#define state(fi) ((struct fh *)(fi->fh))

/* Drop privilege to the calling user */
static void drop_privilege(void) 
{
    struct fuse_context *fc = fuse_get_context();
    umask(fc->umask);
    setegid(fc->gid);
    seteuid(fc->uid);
}

static void regain_privilege(void) 
{
    setegid(getgid());
    seteuid(getuid());
}


/* Simple FUSE wrappers that pass therough to the underlying directory
 * N.B. No access control --> privilege escalation city! */

int stream_getattr(const char *path, struct stat *st)
{
    int rc;
    drop_privilege();
    rc = (stat(fixpath(path), st)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_unlink(const char *path)
{
    int rc;
    drop_privilege();
    rc = (unlink(fixpath(path))) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_rename(const char *old, const char *new)
{
    int rc;
    drop_privilege();
    rc = (rename(fixpath(old), fixpath(new))) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_chmod(const char *path, mode_t mode)
{
    int rc;
    drop_privilege();
    rc = (chmod(fixpath(path), mode)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_chown(const char *path, uid_t uid, gid_t gid)
{
    int rc;
    drop_privilege();
    rc = (chown(fixpath(path), uid, gid)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_truncate(const char *path, off_t off)
{
    int rc;
    drop_privilege();
    rc = (truncate(fixpath(path), off)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_utime(const char *path, struct utimbuf *times)
{
    int rc;
    drop_privilege();
    rc = (utime(fixpath(path), times)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_statfs(const char *path, struct statvfs *buf)
{
    int rc;
    drop_privilege();
    rc = (statvfs(basepath, buf)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_flush(const char *path, struct fuse_file_info *fi)
{
    return 0;
}

int stream_fsync(const char *path, int datasync, struct fuse_file_info *fi)
{
    int rc;
    drop_privilege();
    if (datasync)
        rc = (fdatasync(state(fi)->fd)) ? -errno : 0;
    else
        rc = (fsync(state(fi)->fd)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_access(const char *path, int mode)
{
    int rc;
    drop_privilege();
    rc = (access(fixpath(path), mode)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_ftruncate(const char *path, off_t off, struct fuse_file_info *fi)
{
    int rc;
    drop_privilege();
    rc = (ftruncate(state(fi)->fd, off)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_fgetattr(const char *path, struct stat *st, struct fuse_file_info *fi)
{
    int rc;
    drop_privilege();
    rc = (fstat(state(fi)->fd, st)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_utimens(const char *path, const struct timespec tv[2])
{
    int rc;
    drop_privilege();
    rc = (utimensat(-1, fixpath(path), tv, 0)) ? -errno : 0;
    regain_privilege();
    return rc;
}

/* Directory ops */

int stream_mkdir(const char *path, mode_t mode)
{
    int rc;
    drop_privilege();
    rc = (mkdir(fixpath(path), mode)) ? -errno : 0;
    regain_privilege();
    return rc;
}

int stream_opendir(const char *path, struct fuse_file_info *fi)
{
    int rc = 0;
    char *p = fixpath(path);
    struct fh *fh = malloc(sizeof (struct fh));
    if (!fh) 
        return -ENOMEM;
    struct stream *sp = find_stream(path);
    if (!sp) {
        free(fh);
        return -ENOMEM;
    }
    fh->sp = sp;
    drop_privilege();
    rc = ((fh->fd = open(p, O_RDWR, 0)) < 0) ? -errno : 0;
    if (rc)
        rc = ((fh->fd = open(p, O_RDONLY, 0)) < 0) ? -errno : 0;
    if (!rc)
        rc = ((fh->dirp = fdopendir(fh->fd)) == NULL) ? -errno : -0;
    regain_privilege();
    if (!rc) {
        fi->fh = (uint64_t) fh;
    } else {
        if (fh->fd >= 0)
            close(fh->fd);
        put_stream(sp);
        free(fh);
    }
    return rc;
}

int stream_releasedir(const char *path, struct fuse_file_info *fi)
{
    int rc;
    struct fh *fh = state(fi);
    drop_privilege();
    rc = (closedir(fh->dirp)) ? -errno : -0;
    close(fh->fd);
    regain_privilege();
    fi->fh = 0;
    put_stream(fh->sp);
    free(fh);
    return rc;
}

int stream_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                   off_t off, struct fuse_file_info *fi)
{
    int rc = 0;
    struct dirent *de;
    struct stat st;
    struct fh *fh = state(fi);
    drop_privilege();
    /* XXX this is like the sample code; must do whole dir in one pass */
    while ((errno = 0, de = readdir(fh->dirp)) != NULL) {
        memset(&st, 0, sizeof st);
        st.st_ino = de->d_ino;
        st.st_mode = de->d_type << 12;
        if (filler(buf, de->d_name, &st, 0)) {
            rc = -ENOMEM;
            break;
        }
    }
    if (!rc) 
        rc = -errno;
    regain_privilege();
    return rc;
}

/* File I/O ops */

static int do_stream_open(const char *path, struct fuse_file_info *fi,
                          int extra_flags, mode_t mode)
{
    int rc;
    int flags = fi->flags | extra_flags;
    char *p = fixpath(path);
    struct fh *fh = malloc(sizeof (struct fh));
    if (!fh) 
        return -ENOMEM;
    struct stream *sp = find_stream(path);
    if (!sp) {
        free(fh);
        return -ENOMEM;
    }
    fh->sp = sp;
    drop_privilege();
    rc = ((fh->fd = open(p, flags, mode)) < 0) ? -errno : 0;
    regain_privilege();
    if (!rc)
        fi->fh = (uint64_t) fh;
    else {
        put_stream(sp);
        free(fh);
    }
    return rc;
}

int stream_create(const char *path, mode_t mode, struct fuse_file_info *fi)
{
    return do_stream_open(path, fi, O_CREAT, mode);
}

int stream_open(const char *path, struct fuse_file_info *fi)
{
    return do_stream_open(path, fi, 0, 0);
}

int stream_release(const char *path, struct fuse_file_info *fi)
{
    int rc;
    struct fh *fh = state(fi);
    drop_privilege();
    rc = (close(fh->fd)) ? -errno : 0;
    regain_privilege();
    fi->fh = 0;
    put_stream(fh->sp);
    free(fh);
    return rc;
}

int stream_write(const char *path, const char *buf, size_t len, off_t off,
                 struct fuse_file_info *fi)
{
    int rc = 0;
    struct fh *fh = state(fi);
    if (!fi->writepage) 
        drop_privilege();
    if (!rc) {
        rc = write(fh->fd, buf, len);
        if (rc < 0)
            rc = -errno;
    }
    if (!fi->writepage) 
        regain_privilege();
    return rc;
}


int stream_read(const char *path, char *buf, size_t len, off_t off,
                struct fuse_file_info *fi)
{
    int rc = 0;
    ssize_t done;
    struct fh *fh = state(fi);
again:
    if (!fi->writepage) 
        drop_privilege();
    if (!rc) {
        done = read(fh->fd, buf, len);
        if (done < 0)
            rc = -errno;
        else 
            rc = done;
    }
    if (!fi->writepage) 
        regain_privilege();
    /* The one interesting part of the whole thing */
    if (rc == 0 && fh->sp->streaming) {
        /* XXX Not returning from this read hangs the calling process 
         * XXX hard; another option is to return -EINTR and rely on the
         * XXX caller to retry.  More overhead but easier to unstick. */
        usleep(10000);
        /* XXX Would be better to have a cvar in the sp struct so that we
         * XXX could signal to readers on a write instead of polling. */
        goto again;
    }
    return rc;
}

/* We don't pass ioctls through; just use one to mark the stream ending */
int stream_ioctl(const char *path, int cmd, void *arg,
                 struct fuse_file_info *fi, unsigned int flags, void *data)
{
    struct fh *fh = state(fi);
    fprintf(stderr, "IOCTL %i\n", cmd);
    if (cmd == OPEN_STREAM_IOCTL) {
        pthread_mutex_lock(&streams_mutex);
        fh->sp->streaming++;
        fh->sp->ref++;
        pthread_mutex_unlock(&streams_mutex);
        return 0;
    }
    if (cmd == CLOSE_STREAM_IOCTL) {
        pthread_mutex_lock(&streams_mutex);
        fh->sp->streaming--;
        pthread_mutex_unlock(&streams_mutex);
        put_stream(fh->sp);
        return 0;
    }
    return -EINVAL;
}

/* Boilerplate */

static struct fuse_operations fops = {
    .getattr = stream_getattr,
    .unlink = stream_unlink,
    .rename = stream_rename,
    .chmod = stream_chmod,
    .chown = stream_chown,
    .truncate = stream_truncate,
    .utime = stream_utime,
    .statfs = stream_statfs,
    .flush = stream_flush,
    .fsync = stream_fsync,
    .access = stream_access,
    .ftruncate = stream_ftruncate,
    .fgetattr = stream_fgetattr,
    .utimens = stream_utimens,
    .mkdir = stream_mkdir,
    .opendir = stream_opendir,
    .readdir = stream_readdir,
    .fsyncdir = stream_fsync,
    .releasedir = stream_releasedir,
    .create = stream_create,
    .open = stream_open,
    .read = stream_read,
    .write = stream_write,
    .ioctl = stream_ioctl,
    .release = stream_release,
};

int main(int argc, char **argv)
{
    /* XXX Ought to do better argv-wrangling than this */
    if (argc < 2) {
        fprintf(stderr, "usage: streamfs <basedir> [fuse options]\n");
        return 1;
    }
    basepath = argv[1];
    argv++;
    argc--;
    pthread_mutex_init(&streams_mutex, NULL);
    return fuse_main(argc, argv, &fops, NULL);
}
