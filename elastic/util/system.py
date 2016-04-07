import resource


def get_cureent_memory_usage():
    """
    Get current memory usage in kb

    :return: the maximum residence set in kb
    """
    return resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
