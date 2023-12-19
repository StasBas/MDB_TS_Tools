import os
import sys
import json
from tqdm import tqdm
import argparse
from datetime import datetime

import tkinter as tk

# from tkinter import messagebox

# PATH = "~/Downloads"
PATH = os.getcwd()
FILENAME = None
# FILENAME = "mongodb.log"
# FILENAME = "sampleLog.log"
FROM_TIME = "1970-01-01T00:00:00"
TO_TIME = "2830-01-01T00:00:00"
APP_INFO = True
DRIVER_INFO = True
MAX_PRINT = 10

FILENAME_REQ = False
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"
DECODER_ERROR_LIMIT = 100

# Analysis Search Terms
SEARCH_TERMS = ["Connection accepted", "Connection ended", "client metadata"]


def main():
    # PARAMS
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--path", help=f"Log file path. ex: {os.getcwd()}",
                        default=PATH.get() if isinstance(PATH, tk.StringVar) else PATH)
    parser.add_argument('-f', '--filename', help="Log file name. ex: mongodb.log", required=FILENAME_REQ,
                        default=FILENAME.get() if isinstance(FILENAME, tk.StringVar) else FILENAME)
    parser.add_argument('-s', '--start_time', help="Start time. ex: 1970-01-01T00:00:00",
                        default=FROM_TIME.get() if isinstance(FROM_TIME, tk.StringVar) else FROM_TIME)
    parser.add_argument('-e', '--end_time', help="End time. ex: 3030-01-01T00:00:00",
                        default=TO_TIME.get() if isinstance(TO_TIME, tk.StringVar) else TO_TIME)
    parser.add_argument('-a', '--app_info', help="Collect applications info. ex: 0", type=int,
                        default=APP_INFO.get() if isinstance(APP_INFO, tk.IntVar) else APP_INFO)
    parser.add_argument('-d', '--driver_info', help="Collect drivers info. ex: 1", type=int,
                        default=DRIVER_INFO.get() if isinstance(DRIVER_INFO, tk.IntVar) else DRIVER_INFO)
    parser.add_argument('-ll', '--max_print', help="Result lists print limit. ex: 10.  Use -1 for no limit.",
                        type=int,
                        default=MAX_PRINT.get() if isinstance(MAX_PRINT, tk.IntVar) else MAX_PRINT)
    args = parser.parse_args()
    # Params
    if not args.filename:
        print("For CLI usage: connections_analyzer -h\n")
        window = main_ui()
        window.mainloop()
    else:
        run_analyzer(**vars(args))
    sys.exit(0)


def run_analyzer(*args, **kwargs):
    # # Confirmation Window
    # if len(args) >= 2:
    #     messagebox.showinfo("Confirmation", f"Submitted\n PATH: {args[0].get()}, \nFILENAME: {args[1].get()}")

    print(f"\n{'-' * 3}")
    print("\nConnections analyzer running.")
    for k, v in kwargs.items():
        print(f"\t{k}:{' ' * (15 - len(k))}{v}")
    print("\n")

    path = get_file_path(kwargs['filename'], kwargs['path'])

    analyze_connections(
        log_file_path=path,
        start_time=kwargs['start_time'],
        end_time=kwargs['end_time'],
        app_info=kwargs['app_info'],
        driver_info=kwargs['driver_info'],
        print_limit=kwargs['max_print'],
        decoder_error_limit=kwargs.get('decoder_limit') or DECODER_ERROR_LIMIT
    )


def analyze_connections(log_file_path: str, start_time: str, end_time: str, app_info: True, driver_info: True,
                        print_limit: int = 10, decoder_error_limit: int = DECODER_ERROR_LIMIT):
    """
    :param log_file_path:
    :param start_time:
    :param end_time:
    :param app_info:
    :param driver_info:
    :param print_limit:
    :param decoder_error_limit:
    :return:
    """
    file_stats = os.stat(log_file_path)
    # try:
    #     # TODO: need to remove this and replace with file size property instead
    #     num_lines = sum(1 for _ in open(log_file_path, 'r'))
    #     print(f"Reading \'{log_file_path}\'")
    # except FileNotFoundError:
    #     raise FileNotFoundError(f"No such file: \'{log_file_path}\'")

    # General Params
    first_ts = None
    last_ts = None
    json_decoder_errors_counter = 0

    # Analysis Specific Params
    total_opened = 0
    total_closed = 0
    hosts_stats = dict()  # {host: {open: v, close: k, metadata_keys ....}}
    host_applications = dict()  # {host: [apps]}
    host_drivers = dict()  # {host: [drvs]}

    print("Parsing log")
    import time
    with open(log_file_path, 'r') as f:
        for i, line in enumerate(f):  # (tqdm(f, total=num_lines, file=sys.stdout)):
            print(end=f"\rProcessing line {i} ")
            if line and line != "\n" and line != "\r" and line != "\n\r":

                # Log line to JSON (structured log)
                try:
                    line_json = json.loads(line)
                except json.JSONDecodeError as err:
                    json_decoder_errors_counter += 1
                    if json_decoder_errors_counter < 10:
                        # Few errors may be a result of how the file was written, empty lines etc..
                        pass
                    if json_decoder_errors_counter == 10:
                        # Considerable number of errors - indicates the file may not be properly formatted.
                        pass
                    if json_decoder_errors_counter >= decoder_error_limit:
                        # Exceeds defined tolerance level
                        print(f"\n{json_decoder_errors_counter} log entries failed to decode.")
                        raise RuntimeError(f"\n\"{log_file_path}\" is not a valid MongoDB structured log") from err
                    continue

                # Line TS and Conditions for Analysis
                time_stamp = line_json["t"]["$date"].split(".")[0]
                if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                        and line_json.get("c") == "NETWORK" \
                        and any(term in line for term in SEARCH_TERMS) \
                        and line_json.get("attr") and line_json["attr"].get("remote"):

                    # Update earliest and latest examined time stamps
                    if not first_ts or date_from_string(time_stamp) < date_from_string(first_ts):
                        first_ts = time_stamp
                    if not last_ts or date_from_string(time_stamp) > date_from_string(last_ts):
                        last_ts = time_stamp

                    ####################################################################### PROCESS LINE - START

                    origin_ip = line_json["attr"]["remote"].split(":")[0]

                    # Connection Opened
                    if "Connection accepted" in line_json["msg"]:
                        total_opened += 1
                        if hosts_stats.get(origin_ip):
                            hosts_stats[origin_ip]["opened"] += 1
                        else:
                            hosts_stats[origin_ip] = {"opened": 1, "closed": 0}
                    # Connection Closed
                    elif "Connection ended" in line_json["msg"]:
                        total_closed += 1
                        if hosts_stats.get(origin_ip):
                            hosts_stats[origin_ip]["closed"] += 1
                        else:
                            hosts_stats[origin_ip] = {"opened": 0, "closed": 1}
                    # Connection Metadata Found * May need review if same host (IP) have different metadata *
                    elif "client metadata" in line_json["msg"]:
                        # Get Driver
                        try:
                            drv = line_json["attr"]["doc"].get("driver").get("name")
                        except AttributeError:
                            drv = "unknown"
                        try:
                            drv_ver = line_json["attr"]["doc"].get("driver").get("version")
                        except AttributeError:
                            drv_ver = "unknown"
                        # Get App name
                        try:
                            app = line_json["attr"]["doc"].get("application").get("name")
                        except AttributeError:
                            app = "unknown"
                        # Get OS
                        try:
                            osn = line_json["attr"]["doc"].get("os").get("type")
                        except AttributeError:
                            osn = "unknown"

                        # Add metadata info to host stats
                        # if hosts_stats.get(origin_ip):
                        #     hosts_stats[origin_ip]["driver"] = drv
                        #     hosts_stats[origin_ip]["app"] = app
                        #     hosts_stats[origin_ip]["os"] = osn
                        # else:
                        #     hosts_stats[origin_ip] = {"opened": 0, "closed": 0, "driver": drv,
                        #                               "application": app, "os": osn}

                        # Add Metadata to host applications
                        if app_info:
                            if host_applications.get(origin_ip):
                                if app not in host_applications[origin_ip]:
                                    host_applications[origin_ip].append(app)
                            else:
                                host_applications[origin_ip] = [app]

                        # Add Metadata to host drivers
                        if driver_info:
                            driver_full = f"{drv} {drv_ver}"
                            if host_drivers.get(origin_ip):
                                if driver_full not in host_drivers[origin_ip]:
                                    host_drivers[origin_ip].append(driver_full)
                            else:
                                host_drivers[origin_ip] = [driver_full]

    ####################################################################### PROCESS LINE - END

    # OUTPUT
    print(f"\nShowing up to {print_limit} results for each section, sorted in descending order")
    # Create an f-string for the text
    formatted_text = f"Results from {first_ts} to {last_ts}"
    # Create an f-string for the line of asterisks with the same length
    asterisks_line = f"{'*' * len(formatted_text)}"
    print(formatted_text)
    print(asterisks_line)

    if app_info:
        apps_list_sorted = sorted(host_applications.items(), key=lambda x: len(x[1]), reverse=True)
        print("\nLogged Hosts Applications:")
        for i in apps_list_sorted[:print_limit]:
            print(f"\t{i[0]} ({len(i[1]):,d}): {i[1][:print_limit]}{'...' if len(i[1]) > print_limit > 0 else ''}")
        if len(apps_list_sorted) > print_limit > 0:
            print("\t...")

    if driver_info:
        drivers_list_sorted = sorted(host_drivers.items(), key=lambda x: len(x[1]), reverse=True)
        print("\nLogged Hosts Drivers:")
        for i in drivers_list_sorted[:print_limit]:
            print(f"\t{i[0]} ({len(i[1]):,d}): {i[1][:print_limit]}{'...' if len(i[1]) > print_limit > 0 else ''}")
        if len(drivers_list_sorted) > print_limit > 0:
            print("\t...")

    print("\nHosts Connections Stats:")
    sort_key = "opened"
    connections_list_sorted = sorted(hosts_stats.items(), key=lambda x: x[1][sort_key], reverse=True)
    for i in connections_list_sorted[:print_limit]:
        print(f"\t{i[0]}: Opened {i[1]['opened']:,d}, Closed {i[1]['closed']:,d}, "
              f"Delta {(i[1]['opened'] - i[1]['closed']):,d}")
    if len(connections_list_sorted) > print_limit > 0:
        print("\t...")

    print("\nTotal Connections ({} to {}): \n\tOpened: {:,}\n\tClosed: {:,}\n\tDelta:  {:,}".format(
        first_ts, last_ts, total_opened, total_closed, total_opened - total_closed)
    )

    # Decoder error count warning:
    if json_decoder_errors_counter >= 10:
        print(f"\n\nWARNING: {json_decoder_errors_counter} log lines failed to decode, please verify log file is a "
              f"valid MongoDB structured log file")


def date_from_string(date_str) -> datetime:
    return datetime.strptime(date_str, DATE_FORMAT)


def date_to_string(date_obj: datetime) -> str:
    return datetime.strftime(date_obj, DATE_FORMAT)


def get_file_path(file_name: str, path: str) -> str:
    """If filename is a path leave as is, if not return combination of path and filename"""
    if "~" in path:
        path = path.replace("~", os.path.expanduser('~'))
    if os.sep in file_name:
        return file_name
    else:
        return os.path.join(path, file_name)


def check_file_path(file_name: str, path: str) -> str:
    """Be nice, check if file exists in path, print path contents for visibility"""

    if "~" in path:
        path = path.replace("~", os.path.expanduser('~'))

    if os.sep in file_name:
        print("File name includes path, ignoring path argument")
        path, file_name = file_name.rsplit(os.sep, 1)

    print(f"Looking for \"{file_name}\" in \"{path}\"")

    files = []
    folders = []

    for item in os.listdir(path):
        if os.path.isfile(os.path.join(path, item)):
            files.append(item)
        else:
            folders.append(item)
    print(f"\"{path}\" Folders ({len(folders)}): \n{folders} \n\"{path}\" Files ({len(files)}): \n{files}\n")

    if file_name not in files:
        sys.exit(f"The file \"{file_name}\" is not in \"{path}\"")
    else:
        return os.path.join(path, file_name)


# def print_progress_bar(iteration, total, prefix="Progress", suffix="Complete", decimals=1, length=100, fill='█'):
#     # percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
#     percent = round(100 * (iteration / float(total)), 2)
#     filled_length = int(length * iteration // total)
#     bar = fill * filled_length + '-' * (length - filled_length)
#     print(end=f'\r{prefix} |{bar}| {percent}% {suffix}')
#     # Print New Line on Complete
#     if iteration == total:
#         print()


def main_ui():
    root = tk.Tk()
    root.title("Connection Analyzer Settings")
    # root.geometry('600x400+100+200')

    top_frame = tk.Frame()
    top_frame.pack(side="top")
    entry_text = """
        Connections report sorted by number in a descending order.
        """
    tk.Label(top_frame, text=entry_text, anchor="w").pack()

    # Create Canvas with Scroller
    form_canvas = tk.Canvas(root, width=400, height=300, borderwidth=0, scrollregion=(0, 0, 10, 10))
    form = tk.Frame(root)

    # Make scrollbars bound to root and scrolling form canvas
    h_bar = tk.Scrollbar(root, orient=tk.HORIZONTAL)
    h_bar.pack(side=tk.BOTTOM, fill=tk.X)
    h_bar.config(command=form_canvas.xview)
    v_bar = tk.Scrollbar(root, orient=tk.VERTICAL)
    v_bar.pack(side=tk.RIGHT, fill=tk.Y)
    v_bar.config(command=form_canvas.yview)

    # configure form canvas
    form_canvas.configure(xscrollcommand=h_bar.set, yscrollcommand=v_bar.set)
    form_canvas.pack(side=tk.LEFT, expand=True, fill=tk.BOTH)
    form_canvas.create_window((100, 5), window=form, anchor=tk.NW)
    form.bind("<Configure>", lambda event: form_canvas.configure(scrollregion=form_canvas.bbox("all")))

    buttons_frame = tk.Frame(root)
    # form_canvas.create_window((190, 270), window=buttons_frame)
    buttons_frame.pack(side="bottom")

    # Populate
    # Params
    tk.Label(form, text="Path", anchor="w").grid(row=1, column=5, sticky="w")

    path = tk.StringVar()
    # path.set("/Users/stas.baskin/Downloads")
    # path.set(os.getcwd())
    path.set(os.path.join("~", "Downloads"))
    tk.Entry(form, textvariable=path).grid(row=1, column=10, sticky="w")

    tk.Label(form, text="File Name").grid(row=2, column=5, sticky="w")
    filename = tk.StringVar()
    filename.set("mongodb.log")
    tk.Entry(form, textvariable=filename).grid(row=2, column=10, sticky="w")

    tk.Label(form, text="Start Time").grid(row=3, column=5, sticky="w")
    start_time = tk.StringVar()
    start_time.set("1970-01-01T00:00:00")
    tk.Entry(form, textvariable=start_time).grid(row=3, column=10, sticky="w")

    tk.Label(form, text="End Time").grid(row=4, column=5, sticky="w")
    end_time = tk.StringVar()
    end_time.set("2830-01-01T00:00:00")
    tk.Entry(form, textvariable=end_time).grid(row=4, column=10, sticky="w")

    tk.Label(form, text="Applications Info").grid(row=5, column=5, sticky="w")
    apps_info = tk.BooleanVar()
    apps_info.set(False)
    tk.Checkbutton(form, text='', variable=apps_info, onvalue=1, offvalue=0).grid(row=5, column=10, sticky="w")
    tk.Label(form, text="Print available connection applications metadata").grid(row=5, column=20, sticky="w")

    tk.Label(form, text="Drivers Info").grid(row=6, column=5, sticky="w")
    drivers_info = tk.BooleanVar()
    drivers_info.set(True)
    tk.Checkbutton(
        form, text='', variable=drivers_info, onvalue=1, offvalue=0).grid(
        row=6, column=10, sticky="w")
    tk.Label(
        form, text="Print available connection drivers metadata", anchor="w").grid(
        row=6, column=20, sticky="w")

    tk.Label(form, text="Max Entries").grid(row=7, column=5, sticky="w")
    max_print = tk.IntVar()
    max_print.set(10)
    tk.Entry(form, textvariable=max_print).grid(row=7, column=10, sticky="w")
    tk.Label(form, text="Define number of results to print wrapped with \"...\"",
             anchor="w").grid(row=7, column=20, sticky="w")

    tk.Label(form, text="Decoder Error Limit").grid(row=9, column=5, sticky="w")
    decoder_limit = tk.IntVar()
    decoder_limit.set(100)
    tk.Entry(form, textvariable=decoder_limit).grid(row=9, column=10, sticky="w")
    tk.Label(form, text="Maximum number of decoding errors before an error is raised").grid(
        row=9, column=20, sticky="w")

    # Buttons Frame For Buttons
    tk.Button(buttons_frame, text="Run Analyzer", command=lambda: run_analyzer(
        path=path.get(), filename=filename.get(), start_time=start_time.get(), end_time=end_time.get(),
        app_info=apps_info.get(), driver_info=drivers_info.get(), max_print=max_print.get(),
        decoder_limit=decoder_limit.get()
    )).pack(side="right")

    tk.Button(buttons_frame, text="Exit", command=lambda: exit_form(root)).pack(side='left')

    # Enable use of enter and escape.
    root.bind('<Return>', lambda event: run_analyzer(
        path=path.get(), filename=filename.get(), start_time=start_time.get(), end_time=end_time.get(),
        app_info=apps_info.get(), driver_info=drivers_info.get(), max_print=max_print.get(),
        decoder_limit=decoder_limit.get()
    ))
    root.bind('<Escape>', lambda event: exit_form(root))

    # Return root object
    return root


def exit_form(window: tk.Tk):
    window.destroy()


if __name__ == "__main__":
    main()
