
def log(msg, line_break: bool = True):
    with open('sub_log.txt', 'a') as the_file:
        the_file.write(msg +'\n')
