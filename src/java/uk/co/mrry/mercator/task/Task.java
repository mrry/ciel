/*
 * Copyright (c) 2010 Chris Smowton <chris.smowton@cl.cam.ac.uk>
 * Copyright (c) 2010 Derek Murray <derek.murray@cl.cam.ac.uk>
 * Copyright (c) 2010 Malte Schwarzkopf <ms705@cl.cam.ac.uk>
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
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

package uk.co.mrry.mercator.task;

import java.io.InputStream;
import java.io.OutputStream;


/**
 * A task takes zero or more named data items, performs some computation on them, and yields one or more concrete data items
 * as output.
 * 
 * To execute a task, the named input data must be available locally.
 * 
 * @author dgm36
 *
 */
public interface Task {

    public void invoke(InputStream[] fis, OutputStream[] fos, String[] args);
	
}
