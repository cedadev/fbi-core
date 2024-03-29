import datetime

def sizeof_fmt(num, suffix="B"):
    if num is None:
        return "None"
    for unit in ["", "Ki", "Mi", "Gi", "Ti", "Pi", "Ei", "Zi"]:
        if abs(num) < 1024.0:
            return f"{num:3.1f}{unit}{suffix}"
        num /= 1024.0
    return f"{num:.1f}Yi{suffix}"


def format_time(t):
    dt = datetime.datetime.fromtimestamp(t)
    return dt.strftime("%Y-%m-%d %H:%M:%S")


