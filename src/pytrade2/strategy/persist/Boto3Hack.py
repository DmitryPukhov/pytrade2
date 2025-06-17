import concurrent.futures.thread
import functools
import threading
from concurrent.futures import _base
from concurrent.futures.thread import BrokenThreadPool


class Boto3Hack:
    """
    Hack to fix boto3 threading issue: https://bugs.python.org/issue42647
    """

    @staticmethod
    def submit(self, fn, /, *args, **kwargs):
        """
            Hack to fix boto3 threading issue: https://bugs.python.org/issue42647
            Works for Python 3.11
            """

        # This code is commented to fix boto3 error
        with self._shutdown_lock:  # _global_shutdown_lock:
            if self._broken:
                raise BrokenThreadPool(self._broken)

            if self._shutdown:
                raise RuntimeError('cannot schedule new futures after shutdown')
            # This code is commented to fix boto3 error
            # if _shutdown:
            #     raise RuntimeError('cannot schedule new futures after '
            #                        'interpreter shutdown')

            f = _base.Future()
            w = concurrent.futures.thread._WorkItem(f, fn, args, kwargs)

            self._work_queue.put(w)
            self._adjust_thread_count()
            return f

    @classmethod
    def _register_atexit(cls, func, *arg, **kwargs):
        """
        Hack to fix boto3 threading issue: https://bugs.python.org/issue42647
        """
        # This code is commented to fix boto3 error
        # if threading._SHUTTING_DOWN:
        #     raise RuntimeError("can't register atexit after shutdown")

        call = functools.partial(func, *arg, **kwargs)
        threading._threading_atexits.append(call)
