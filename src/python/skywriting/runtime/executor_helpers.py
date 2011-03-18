
class ContextManager:
    def __init__(self, description):
        self.description = description
        self.active_contexts = []

    def add_context(self, new_context):
        ret = new_context.__enter__()
        self.active_contexts.append(ret)
        return ret
    
    def remove_context(self, context):
        self.active_contexts.remove(context)
        context.__exit__(None, None, None)

    def __enter__(self):
        return self

    def __exit__(self, exnt, exnv, exnbt):
        if exnt is not None:
            ciel.log("Context manager for %s exiting with exception %s" % (self.description, repr(exnv)), "EXEC", logging.WARNING)
        else:
            ciel.log("Context manager for %s exiting cleanly" % self.description, "EXEC", logging.INFO)
        for ctx in self.active_contexts:
            ctx.__exit__(exnt, exnv, exnbt)
        return False
