import time
from datetime import datetime

class Logger:

    def __init__(self, log_file: str = 'sub_monitor.log', msg_rate_log_file: str = 'sub_msg_rate.log', print_log: bool = True, print_msg_rate: bool = False):
        self.log_file: str = log_file
        self.msg_rate_log_file: str = msg_rate_log_file
        self.b_print_log: bool = print_log
        self.b_print_msg_rate_log: bool = print_msg_rate

    def get_log_prefix(self, level: str) -> str:
        return datetime.utcnow().strftime('%y/%m/%d %H:%M:%S') + ' ['+ level +'] '

    def print_log(self, to_log_str: str) -> None:
        if (self.b_print_log == True):
            print(to_log_str)

    def write_log(self, to_log_str: str) -> None:
        with open(self.log_file, 'a') as the_file:
            the_file.write(to_log_str + '\n')

    def write_msg_rate_log(self, to_log_str: str) -> None:
        with open(self.msg_rate_log_file, 'a') as the_file:
            the_file.write(to_log_str + '\n')

    def debug(self, text: str):
        to_log_str = self.get_log_prefix('DEBUG') + text
        self.print_log(to_log_str)
        self.write_log(to_log_str)

    def info(self, text: str):
        to_log_str = self.get_log_prefix('INFO') + text
        self.print_log(to_log_str)
        self.write_log(to_log_str)

    def warning(self, text: str):
        to_log_str = self.get_log_prefix('WARNING') + text
        self.print_log(to_log_str)
        self.write_log(to_log_str)

    def error(self, text: str):
        to_log_str = self.get_log_prefix('ERROR') + text
        self.print_log(to_log_str)
        self.write_log(to_log_str)

    def msg_rate_info(self, text):
        to_log_str = self.get_log_prefix('INFO') + text
        if (self.b_print_msg_rate_log == True): 
            self.print_log(to_log_str)
        self.write_msg_rate_log(to_log_str)

    def msg_rate_waring(self, text):
        to_log_str = self.get_log_prefix('WARNING') + text
        if (self.b_print_msg_rate_log == True): 
            self.print_log(to_log_str)
        self.write_msg_rate_log(to_log_str)
