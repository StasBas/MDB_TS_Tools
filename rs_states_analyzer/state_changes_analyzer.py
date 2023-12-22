import json
import time
import argparse
import os
import multiprocessing

from multiprocessing import Queue, Event, Process, Manager
from threading import Thread

from utils.obj import FormTemplate
from utils.ops import print_progress_bar, date_from_string

PATH = None  # "~/Downloads/zoharSample.log"
TIME_S = "1970-01-01T00:00:00"
TIME_E = "2071-10-11T00:00:00"
DECODER_ERR_MAX = 100
MAX_FILE_SIZE = 3 * (10 ** 9)

WORKERS = os.cpu_count()

STATE_CHANGES = True


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--path", help=f"Log file path. ex: \"~/Downloads/mongodb.log\"",
                        required=False, default=PATH)
    parser.add_argument('-s', "--start_time", help=f"Start Time. ex: \"1970-01-01T00:00:00\"",
                        required=False, default=TIME_S)
    parser.add_argument('-r', "--end_time", help=f"End Time. ex: \"2071-10-11T00:00:00\"",
                        required=False, default=TIME_E)
    parser.add_argument('-l', "--error_limit", help=f"Error limit. ex: 100", type=int,
                        required=False, default=DECODER_ERR_MAX)
    parser.add_argument('-w', "--workers", help=f"concurrency. ex: 10", type=int,
                        required=False, default=WORKERS)
    parser.add_argument('-st', "--state_changes", help=f"analyzer for state changes. ex: 1/0", type=bool,
                        required=False, default=STATE_CHANGES)
    args = parser.parse_args()

    if args.path and os.path.sep not in args.path:
        args.path = os.path.join(os.getcwd(), args.path)

    if args.path:
        analyzer_config(**vars(args))
    else:
        form().open()


def form():
    root = FormTemplate()
    root.add_title("Log Analyzer")
    root.add_wellcome_message("State Transition Changes")

    log_path = root.add_text_field(name="Log File Path", default="~/Downloads/mongodb.log", info="Path to log file")
    start_time = root.add_text_field(name="Start Time", default="1970-01-01T00:00:00",
                                     info="Start time (yyyy-mm-ddTHH-MM-SS)")
    end_time = root.add_text_field(name="End Tme", default="2071-10-11T00:00:00", info="End time (yyyy-mm-ddTHH-MM-SS)")
    # decoder_error_limit = root.add_num_field(name="Decoder Error Limit", default=100,
    #                                          info="Max allowed decoder errors")
    # workers = root.add_num_field(name="Concurrent workers", default=5, info="Concurrent workers")
    # root.add_separator()
    # state_changes = root.add_bool_field(name="Analyze State Changes", default=True)

    root.set_action_button(
        "Run Analyzer",
        lambda: analyzer_config(
            path=log_path.get(),
            start_time=start_time.get(),
            end_time=end_time.get(),
            error_limit=DECODER_ERR_MAX,  # decoder_error_limit.get()
            workers=WORKERS,  # workers.get()
            state_changes=STATE_CHANGES,  # state_changes.get()
        )
    )

    return root


def analyzer_config(*args, **kwargs):
    print(f"\n{'-' * 3}\n\nAnalyzer Parameters:")
    for arg in args:
        print(f"\t{arg}")
    for k, v in kwargs.items():
        print(f"\t{k}:{(15 - len(k)) * ' '}{v}")
    print("\n")

    if "~" in kwargs['path']:
        kwargs['path'] = kwargs['path'].replace('~', os.path.expanduser('~'))

    analyzer_executor(
        path=kwargs['path'],
        err_limit=kwargs['error_limit'],
        start_time=kwargs['start_time'],
        end_time=kwargs['end_time'],
        workers=kwargs['workers'],
        state_changes=kwargs['state_changes']
    )


def analyzer_executor(path, start_time, end_time, err_limit, workers,
                      state_changes):
    queue_read = Queue()
    queue_parsed = Queue()

    read_done_event = Event()
    parse_done_event = Event()
    reports_done_event = Event()

    state_changes_report_raw = list()
    state_changes_report = list()
    state_analyzer_keywords = ['Member is now in state RS_DOWN',
                               'Member is in new state',
                               'Replica set state transition']

    try:
        file_size = os.stat(path).st_size
    except FileNotFoundError:
        raise FileNotFoundError(f"No such file: \'{path}\'")

    if file_size < MAX_FILE_SIZE:
        with open(path) as f:
            print(f"Reading \'{path}\'")
            lines = f.readlines()
        g_args = (queue_read, read_done_event, lines)
    else:
        g_args = (queue_read, read_done_event, path)

    tstart = time.time()
    file_reader = Process(target=task_read_file,
                          args=g_args,
                          daemon=False)
    file_reader.start()

    report_builder = Thread(
        target=task_build_reports,
        args=(queue_parsed, parse_done_event,
              reports_done_event,
              state_changes_report, state_changes_report_raw,),
        daemon=False,
    )
    report_builder.start()

    procs = []
    for i in range(workers):
        p = Process(target=task_main_log_read,
                    args=(queue_read, read_done_event,
                          queue_parsed, parse_done_event,
                          err_limit, path,
                          start_time, end_time,
                          state_changes, state_analyzer_keywords),
                    daemon=False,
                    )
        procs.append(p)
        p.start()

    while not reports_done_event.is_set():
        if read_done_event.is_set():
            print(end=f"\rParsing log {int(time.time()-tstart)}, "
                      f"Read: {read_done_event.is_set()}, "
                      f"Parse Done: {parse_done_event.is_set()}, "
                      f"Reports Done {reports_done_event.is_set()} ")

        for p in procs:
            if p.exitcode:
                p.join(0)
                procs.remove(p)
                print(f"\nProcess {p.pid} exited with code {p.exitcode}\n")
            # Check if all processes failed
        if len(procs) == 0:
            raise RuntimeError(f"\n*** All processes failed. Stopping! ***"
                               f"\nPlease review the output for the failure errors")

        time.sleep(1)

    print(end=f"\rParsing log {int(time.time() - tstart)}, "
              f"Read: {read_done_event.is_set()}, "
              f"Parse Done: {parse_done_event.is_set()}, "
              f"Reports Done {reports_done_event.is_set()} ")

    for p in procs:
        p.terminate()

    print("\nAnalysis complete\n")
    analyzer_reporter(state_changes, state_changes_report, state_changes_report_raw)


def analyzer_reporter(state_changes, state_changes_report, state_changes_report_raw):
    if state_changes:
        print(f"State Changes Results:\n{'-'*22}\n\n")
        if len(state_changes_report_raw) == 0:
            print("\tNo state changes logged")
        # elif print_raw_results:
        #     for item in state_changes_report_raw:
        #         item = item.replace('\n', '')
        #         print(f"\t{item}")
        else:
            for item in state_changes_report:
                print(f"\t{item}")


def parser_state_changes(line, line_json, queue_parsed: Queue):

    ts = line_json["t"]["$date"]
    result = line

    if line_json.get('msg') == "Replica set state transition":
        result = f"[{ts}] Member: SELF, State Changed to: {line_json['attr']['newState']} " \
                 f"from:{line_json['attr']['oldState']}"
    elif line_json.get('msg') == "Member is now in state RS_DOWN":
        result = f"[{ts}] Member: {line_json['attr']['hostAndPort']}, State Changed to: RS_DOWN"
    elif line_json.get('msg') == "Member is in new state":
        result = f"[{ts}] Member: {line_json['attr']['hostAndPort']}, State Changed to: {line_json['attr']['newState']}"

    queue_parsed.put({"report": "state_changes", "result": result, "raw": line})


def task_read_file(queue_read: Queue, read_done_event: Event, file, ):
    if isinstance(file, list):
        print(f"Reading log")
        for i, line in enumerate(file):
            print_progress_bar(i + 1, len(file), length=30)
            queue_read.put(line)
        read_done_event.set()
    else:
        print(f"reading {file}")
        with open(file) as f:
            for i, line in enumerate(f):
                print(end=f"\rReading line \'{i}\'")
                queue_read.put(line)
        print(f"\rLines Read: {i}")
        read_done_event.set()


def task_build_reports(queue_parsed: Queue, parse_done_event: Event,
                       reports_done_event: Event,
                       state_changes_report: list, state_changes_report_raw: list):
    while not reports_done_event.is_set():
        if queue_parsed.empty():
            if parse_done_event.is_set():
                reports_done_event.set()
            else:
                time.sleep(0.1)
                continue
        else:
            item = queue_parsed.get()
            report = item['report']
            result = item['result']

            if report == "state_changes":
                state_changes_report.append(result)
                state_changes_report_raw.append(item['raw'])


def task_main_log_read(queue_read: Queue, read_done_event: Event,
                       queue_parsed: Queue, parse_done_event: Event,
                       err_limit: int, path: str,
                       start_time, end_time,
                       state_changes, state_analyzer_keywords):

    json_decoder_errors_counter = 0

    while not parse_done_event.is_set():
        if queue_read.empty():
            if read_done_event.is_set():
                parse_done_event.set()
            else:
                time.sleep(0.1)
                continue
        else:
            line = queue_read.get()
            work_threads = list()

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
                if json_decoder_errors_counter >= err_limit:
                    # Exceeds defined tolerance level
                    print(f"\n{json_decoder_errors_counter} log entries failed to decode.")
                    raise RuntimeError(f"\n\"{path}\" is not a valid MongoDB structured log") from err
                continue

            # Line TS and Conditions for Analysis
            time_stamp = line_json["t"]["$date"].split(".")[0]

            ###############
            # PARSE EVENT #
            ###############
            if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time):
                if any(term in line for term in state_analyzer_keywords):
                    t = Thread(
                        target=parser_state_changes,
                        args=(line, line_json, queue_parsed),
                        daemon=False
                    )
                    t.start()
                    work_threads.append(t)

            # Wait for workers to finish
            for t in work_threads:
                t.join()


if __name__ == '__main__':
    multiprocessing.freeze_support()
    main()
