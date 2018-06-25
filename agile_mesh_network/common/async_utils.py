def future_set_result_silent(fut, result):
    if not fut.done():
        fut.set_result(result)


def future_set_exception_silent(fut, exception):
    if not fut.done():
        fut.set_exception(exception)
