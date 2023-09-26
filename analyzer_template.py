import os
import json
import time
import argparse
import multiprocessing

from multiprocessing import Event, Queue, Pool
from datetime import datetime
from threading import Thread

from utils.obj import FormTemplate

PATH = None
TIME_S = "1970-01-01T00:00:00"
TIME_E = "2077-10-31T11:03:00"
CONCURRENCY = 3

CONDITION = "COLLSCAN"  # TODO: Remove Sample

DECODER_ERROR_LIMIT = 100
DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--path", help=f"Log file path. ex: \"~/Downloads/mongodb.log\"", default=PATH)
    parser.add_argument('-s', "--start_time", help=f"Start Time. ex: \"1970-01-01T00:00:00\"",
                        required=False, default=TIME_S)
    parser.add_argument('-e', "--end_time", help=f"End Time. ex: \"3000-01-01T00:00:00\"",
                        required=False, default=TIME_E)
    parser.add_argument('-w', "--workers", help=f"Workers (cores) . ex: 4", type=int,
                        required=False, default=CONCURRENCY or int(os.cpu_count() / 2))

    parser.add_argument('-t', "--trigger1", help=f"Sample", default=CONDITION)  # TODO: Remove Sample

    args = parser.parse_args()

    if args.path:
        if os.path.sep not in args.path:
            args.path = os.path.join(os.getcwd(), args.path)
        analyzer_config(**vars(args))
    else:
        form().open()


def form():
    root = FormTemplate(sizey=300)
    root.add_title("Log Analyzer")
    root.add_wellcome_message("Template")

    log_path = root.add_text_field(name="Log File Path", default="~/Downloads/mongodb.log", info="Path to log file")
    start_time = root.add_text_field(name="Start Time", default="1970-01-01T00:00:00", info="Start time")
    end_time = root.add_text_field(name="End Tme", default="2077-10-31T11:03:00", info="End time")
    workers = root.add_num_field(name="Workers", default=CONCURRENCY or int(os.cpu_count() / 2),
                                 info="Concurrency")
    root.add_separator()

    trigger1 = root.add_text_field(name="sample analyzer", default=CONDITION, info="sample")  # TODO: replace sample

    root.set_action_button(
        "Run Analyzer",
        lambda: analyzer_config(
            path=log_path.get(),
            start_time=start_time.get(),
            end_time=end_time.get(),
            workers=workers.get(),
            trigger1=trigger1.get()
        )
    )

    return root


def analyzer_config(*args, **kwargs):
    print(f"\n{'-' * 3}\n\nAnalyzer Parameters:")
    run_params = ""
    for arg in args:
        run_params += f"\t{arg}\n"
    for k, v in kwargs.items():
        run_params += f"\t{k}:{(17 - len(k)) * ' '}{v}\n"
    print(f"{run_params}\n")

    kwargs['path'] = kwargs['path'].replace('~', os.path.expanduser('~'))

    analyzer_executor(
        path=kwargs['path'],
        start_time=kwargs['start_time'],
        end_time=kwargs['end_time'],
        workers=kwargs['workers'],
        trigger1=kwargs.get('trigger1'),  # TODO: Replace sample
    )


def analyzer_executor(path, start_time, end_time, workers,
                      trigger1):  # TODO: Replace sample

    procs = []

    log_lines_queue = Queue()
    results_queue = Queue()
    read_done = Event()
    parse_done = Event()
    reports_done = Event()
    reports_failed = Event()

    report1 = dict(ns={}, lines=[])  # TODO: Replace sample

    read_thread = Thread(target=task_read_file,
                         args=(path, log_lines_queue, read_done),
                         daemon=True)
    read_thread.start()

    report_thread = Thread(target=task_build_reports,
                           args=(report1, results_queue, read_done, reports_done, reports_failed),
                           daemon=True)
    report_thread.start()

    stso = datetime.strptime(start_time, DATE_FORMAT)
    etso = datetime.strptime(end_time, DATE_FORMAT)

    exec_start = time.time()
    for i in range(workers):
        p = multiprocessing.Process(target=task_handle_line,
                                    args=(log_lines_queue, results_queue,
                                          read_done, parse_done,
                                          stso, etso, path,
                                          trigger1, ),
                                    )
        procs.append(p)
        p.start()

    while not reports_done.is_set():
        if read_done.is_set():
            print(end=f"\rParsing Log: {round(time.time() - exec_start)}s.")

        if reports_failed.is_set():
            for p in procs:
                p.terminate()
            raise RuntimeError(f"\n*** Report Builder Failed. Stopping.***"
                               f"\nPlease review the output for the failure errors")

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

    for p in procs:
        p.terminate()

    print(f"\rDone! Elapsed: {int(time.time()-exec_start)} seconds.\n")

    analyzer_output(trigger1, report1)


def analyzer_output(trigger1, report1):
    if trigger1:  # TODO: Replace sample
        report1_output = ""
        report1_output += f"\nNamespaces matching {trigger1}:\n\n"
        for k, v in dict(sorted(report1['ns'].items(), key=lambda x: x[1], reverse=True)).items():
            report1_output += f"\t{k}: {v}\n"
        report1_output += "\n"
        print(report1_output)


def task_read_file(path, log_lines: Queue, read_done: Event):
    with open(path, 'r') as f:
        for i, line in enumerate(f):
            print(end=f"\rLines Read: {i:,d}")  # TODO: progress bar
            log_lines.put(line)
    print("\n")
    read_done.set()


def task_handle_line(log_lines_queue: Queue, results_queue: Queue,
                     read_done: Event, parse_done: Event,
                     start_time, end_time, path,
                     trigger1):
    decoder_errors = 0

    while not parse_done.is_set():
        if log_lines_queue.empty():
            if read_done.is_set():
                parse_done.set()
            else:
                time.sleep(1)
        else:
            line = log_lines_queue.get()

            try:
                line_json = json.loads(line)
            except json.JSONDecodeError as err:
                decoder_errors += 1
                if decoder_errors >= DECODER_ERROR_LIMIT:
                    print(f"\n{decoder_errors} log entries failed to decode.")
                    raise RuntimeError(f"\n\"{path}\" is not a valid MongoDB structured log") from err
                continue

            threads = []
            time_stamp = line_json["t"]["$date"].split(".")[0]
            tso = datetime.strptime(time_stamp, DATE_FORMAT)

            if end_time > tso > start_time:

                if trigger1 and trigger1 in line:
                    t = Thread(target=parser_trigger1, args=(line, line_json, results_queue), daemon=True)
                    t.start()
                    threads.append(t)

            for t in threads:
                t.join()


def task_build_reports(report1, results: Queue, read_done: Event, reports_done: Event, reports_failed: Event):
    try:
        while not reports_done.is_set():
            if results.empty():
                if read_done.is_set():
                    reports_done.set()
                else:
                    time.sleep(1)
            else:
                item = results.get()
                try:
                    report_name = item['report_name']
                    result = item['result']
                except KeyError as e:
                    reports_failed.set()
                    raise e

                if report_name == "trigger1":
                    if report1['ns'].get(result['ns']):
                        report1['ns'][result['ns']] += 1
                    else:
                        report1['ns'][result['ns']] = 1

                    report1['lines'].append(result['line'])
                else:
                    raise NotImplementedError(f"Report name \"{report_name}\" not parsed")

    except Exception as e:
        reports_failed.set()
        raise e


def parser_trigger1(line, line_json, results_queue):  # TODO: Replace sample
    ns = line_json['attr'].get('ns')
    time_stamp = line_json["t"]["$date"].split(".")[0]

    results_queue.put(
        {
            "report_name": "trigger1",
            "result": {
                "ts": time_stamp,
                "ns": ns,
                "line": line.replace("\n", ""),
            },
        }
    )


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
