import traceback


class HuobiTools:
    @staticmethod
    def format_exception(source, ex):
        tb_msg = None
        if hasattr(ex, "__traceback__"):
            tb_msg = traceback.format_tb(ex.__traceback__)
        return f"Error callback triggered. Source:{source}. Error: {ex}. Traceback: {tb_msg}"