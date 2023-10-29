import json
import argparse
import multiprocessing
import os
import time

from threading import Thread
from multiprocessing import Event, Queue
from datetime import datetime

from utils.obj import FormTemplate
from utils.ops import print_progress_bar, date_from_string, devalue_json

PATH = None  # "~/Downloads/sampleLog.log"
TIME_S = "1970-01-01T00:00:00"
TIME_E = "2077-10-31T11:03:00"
DECODER_ERR_MAX = 100
MAX_PRINT = 10
MAX_LOG_PRINT = 1
OUTPUT_PATH = None  # "~/Documents/reports"
KEY_SEARCH = None  # "bytesRead"
OVERWRITE_REPORTS = False
WORKERS = int(os.cpu_count()/2)

SEARCH_TERMS = ""
SEARCH = ""
RATIO = False


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-p', "--path", help=f"Log file path. ex: \"~/Downloads/mongodb.log\"",
                        required=False, default=PATH)
    parser.add_argument('-s', "--start_time", help=f"Start Time. ex: \"1970-01-01T00:00:00\"",
                        required=False, default=TIME_S)
    parser.add_argument('-r', "--end_time", help=f"End Time. ex: \"3000-01-01T00:00:00\"",
                        required=False, default=TIME_E)
    parser.add_argument('-l', "--error_limit", help=f"Error limit. ex: 100", type=int,
                        required=False, default=DECODER_ERR_MAX)
    parser.add_argument('-pl', "--max_print", help=f"Max results to print. ex: 10", type=int,
                        required=False, default=MAX_PRINT)
    parser.add_argument('-log_examples', "--log_examples", help=f"Max log lines to print. ex: 10", type=int,
                        required=False, default=MAX_LOG_PRINT)
    parser.add_argument('-output', "--output_path", help=f"Path to save reports. ex: \"~/Documents/reports\"",
                        required=False, default=OUTPUT_PATH)
    # parser.add_argument('-st', "--search_terms", help=f"Comma separated search terms. ex:\"COLLSCAN,hasSortStage\"",
    #                     required=False, default=SEARCH_TERMS)
    parser.add_argument('-fs', "--search", help=f"Full search. All terms must be in line. Terms starting with \"-\""
                                                f" must not be in line ex:\"-COLLSCAN,hasSortStage\"",
                        required=False, default=SEARCH)
    parser.add_argument('-ks', "--key_search", help=f"Search for pecific key values ex: bytesRead",
                        required=False, default=KEY_SEARCH)
    parser.add_argument('-ratio', "--ratio", help=f"Include ratio analysis . ex: 1/0", type=bool,
                        required=False, default=RATIO)
    parser.add_argument('-w', "--workers", help=f"Workers (cores) . ex: 4", type=int,
                        required=False, default=WORKERS)
    args = parser.parse_args()

    if args.path and os.path.sep not in args.path:
        args.path = os.path.join(os.getcwd(), args.path)

    print("For CLI usage run \"queries_analyzer -h\".")

    if args.path:
        analyzer_config(**vars(args))
    else:
        form().open()


def form():
    root = FormTemplate(sizey=350)
    root.add_title("Log Analyzer")
    root.add_wellcome_message("Queries Analyzer")

    log_path = root.add_text_field(name="Log File Path", default="~/Downloads/mongodb.log", info="Path to log file")
    start_time = root.add_text_field(name="Start Time", default="1970-01-01T00:00:00", info="Start time")
    end_time = root.add_text_field(name="End Tme", default="2077-10-31T11:03:00", info="End time")
    decoder_error_limit = root.add_num_field(name="Decoder Error Limit", default=100, info="Max allowed decoder errors")
    max_print = root.add_num_field(name="Max print length", default=10, info="Max lines to print per report")
    log_examples = root.add_num_field(name="Log Examples", default=1, info="Log examples to include in report")
    workers = root.add_num_field(name="Workers", default=int(os.cpu_count()/2),
                                 info="Workers (Probably shouldn't exceed CPU count)")
    output_path = root.add_text_field(name="Output Path", default="", info="Path to save report files in. ex:"
                                                                           "\"~/Documents/Reports\"."
                                                                           "Leave empty to print results in console.")
    root.add_separator()

    # search_terms = root.add_text_field(name="Single Search Terms",
    #                                    default="",
    #                                    info="Comma separated terms to scan the log for. No Spaces. "
    #                                         "Checks log for each term separately. "
    #                                         "ex: COLLSCAN,hasSortStage,writeConflicts,regex")
    search = root.add_text_field(name="Text Search",
                                 default="",
                                 info="Search text in log. Comma separated (without spaces) terms"
                                      "Use \"-\" for terms to exclude."
                                      "ex: \"hasSortStage,-COLLSCAN\" (all blocking sort ops that are not COLLSCAN).")

    key_search = root.add_text_field(name="Attribute Search", default="",
                                     info="Provide statistics for specific attribute in the log."
                                          "ex: bytesRead, durationMillis")
    ratio = root.add_bool_field(name="Perform Ratio Analysis", default=False,
                                info="Provide queries ratio statistics.")

    root.set_action_button(
        "Run Analyzer",
        lambda: analyzer_config(
            path=log_path.get(),
            start_time=start_time.get(),
            end_time=end_time.get(),
            error_limit=decoder_error_limit.get(),
            max_print=max_print.get(),
            # search_terms=search_terms.get(),
            log_examples=log_examples.get(),
            output_path=output_path.get(),
            workers=workers.get(),
            search=search.get(),
            ratio=ratio.get(),
            key_search=key_search.get(),
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
    if kwargs.get("output_path"):
        kwargs['output_path'] = kwargs['output_path'].replace('~', os.path.expanduser('~'))

    # search_terms = kwargs['search_terms'].split(",") if kwargs['search_terms'] else None

    search = kwargs['search'].split(",") if kwargs.get('search') and kwargs['search'] else None

    if not kwargs.get('key_search') and not search and not kwargs.get('ratio'):  # and not search_terms:
        for i in range(4):
            print(end=f"\r{'.' * (i + 1)}")
            time.sleep(0.3)
        print(end="\rNo analysis selected.")
        time.sleep(0.5)
        print(end="\rThank you.")
        time.sleep(0.5)
        print(end="\rNext!")
    else:
        analyzer_executor(
            path=kwargs['path'],
            err_limit=kwargs['error_limit'],
            start_time=kwargs['start_time'],
            end_time=kwargs['end_time'],
            max_print=kwargs['max_print'],
            log_examples=kwargs.get('log_examples') if kwargs.get('log_examples') else 0,
            output_path=kwargs.get('output_path'),
            workers=kwargs.get('workers'),
            # search_terms=search_terms,
            ratio=kwargs['ratio'],
            ratio_threshold=kwargs.get('ratio_threshold') or 1000,
            search=search,
            key_search=kwargs.get('key_search'),
        )


def analyzer_executor(path, start_time, end_time, search: list, workers=1,
                      err_limit: int = 100, max_print: int = 10, ratio: bool = False,
                      ratio_threshold=1000, key_search=None, log_examples=0, output_path=None):
    qin = Queue()
    qout = Queue()
    qin_done = Event()
    qout_done = Event()
    reports_done = Event()
    reports_failed = Event()

    ex_start_time = time.time()

    ratio_report = list()
    key_search_report = list()

    time_stamps = dict(min_ts=None, max_ts=None)

    # Search Report
    fs_report = dict(examples=list(), ns=dict(), query_shapes=list())
    search_include, search_exclude = list(), list()
    if search:
        for st in search:
            if st.startswith("-"):
                search_exclude.append(st.replace('-', ''))
            else:
                search_include.append(st)

    # # Search Terms Report
    # search_terms_report = dict()
    # if search_terms:
    #     for term in search_terms:
    #         search_terms_report[term] = {
    #             "examples": list(),
    #             "ns": dict(),
    #             'query_shapes': list()
    #         }

    try:
        with open(path) as f:
            print(f"Reading \'{path}\'")
            log_file = f.readlines()
    except FileNotFoundError:
        raise FileNotFoundError(f"No such file: \'{path}\'")

    qin_generator = Thread(target=task_generate_queue,
                           args=(qin, qin_done, log_file),
                           daemon=True)
    qin_generator.start()

    report_handler = Thread(target=task_generate_reports,
                            args=(qout, qout_done, reports_done, reports_failed,
                                  ratio_report, key_search_report, fs_report),
                            daemon=True)
    report_handler.start()

    procs = list()
    for i in range(workers):
        p = multiprocessing.Process(target=task_parse_log,
                                    args=(qin, qout, qout_done, qin_done,
                                          path, err_limit, start_time, end_time,
                                          ratio,
                                          key_search,
                                          search_include, search_exclude, ),
                                    )
        procs.append(p)
        p.start()

    while not reports_done.is_set():
        if qin_done.is_set():
            print(end=f"\rParsing Log: {round(time.time() - ex_start_time)}s, "
                      f"Read Done: {qin_done.is_set()}, "
                      f"Parse Done: {qout_done.is_set()}, "
                      f"Reports Done: {reports_done.is_set()}")

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

    print(end=f"\rElapsed: {round(time.time() - ex_start_time)}s, "
              f"Read Done: {qin_done.is_set()}, "
              f"Parse Done: {qout_done.is_set()}, "
              f"Reports Done: {reports_done.is_set()}"
              f"\n")

    analyzer_reporter(max_print=max_print, output_path=output_path, log_examples=log_examples,
                      ratio=ratio, ratio_report=ratio_report,
                      key_search=key_search, key_search_report=key_search_report,
                      search=search, fs_report=fs_report, )


def analyzer_reporter(max_print, output_path, log_examples,
                      ratio, ratio_report,
                      key_search, key_search_report,
                      search, fs_report, ):
    ##########
    # OUTPUT #
    ##########
    # print(f"\nAnalysis Done in {exec_duration} seconds. Printing results from {min_ts} to {max_ts}\n")

    if output_path:
        if not os.path.exists(output_path):
            os.mkdir(output_path)

    # ######################
    # # Search Term report #
    # ######################
    # if search_terms:
    #     search_terms_output = ""
    #     search_terms_output += f"\n{'*' * 23}\n* Search Terms Report *\n{'*' * 23}\n\n"
    #
    #     for k, v in search_terms_report.items():
    #         search_terms_output += f"\nSearch Term \"{k}\" yielded {len(v['examples'])} results.\n"
    #         # Namespaces
    #         search_terms_output += f"\n\tTop {max_print} NameSpaces (out of {len(v['ns'])}):\n"
    #         for ns, count in dict(sorted(v['ns'].items(), key=lambda x: x[1], reverse=True)[:max_print]).items():
    #             search_terms_output += f"\t\t{ns}: {count}\n"
    #         # Query Shapes
    #         search_terms_output += f"\n\tTop {max_print} Query Shapes (out of {len(v['query_shapes'])}):\n"
    #         for fs in sorted(v['query_shapes'], key=lambda x: x['count'], reverse=True)[:max_print]:
    #             search_terms_output += f"\t\t{json.dumps(fs)}\n"
    #         # Example Queries
    #         search_terms_output += f"\n\tExamples ({log_examples} of {len(v['examples'])}):\n"
    #         for example in v['examples'][:log_examples]:
    #             search_terms_output += example
    #     util_handle_report_output(search_terms_output, "search_terms_report.txt", OVERWRITE_REPORTS, output_path)

    #################
    # SEARCH REPORT #
    #################
    if search:
        search_output = ""
        search_output += f"\n{'*' * 18}\n* Search Results *\n{'*' * 18}\n\n"
        search_output += f"Got {len(fs_report['examples'])} for search {search}\n"

        search_output += f"\n\tTop {max_print} NameSpaces (out of {len(fs_report['ns'])}):\n"
        for ns, count in dict(sorted(fs_report['ns'].items(), key=lambda x: x[1], reverse=True)[:max_print]).items():
            search_output += f"\t\t{ns}: {count}\n"

        search_output += f"\n\tTop {max_print} Query Shapes (out of {len(fs_report['query_shapes'])}):\n"
        for qs in enumerate(sorted(fs_report['query_shapes'], key=lambda x: x['Count'], reverse=True)[:max_print]):
            search_output += f"\t\tQuery Shape {qs[0] + 1}:\n"
            for k, v in qs[1].items():
                search_output += f"\t\t\t{k}: {v}\n"
            search_output += "\n"
            # search_output.append(f"\t\t{json.dumps(qs)}")

        search_output += f"\n\tText Search Log Examples ({log_examples} of {len(fs_report['examples'])}):\n"
        for example in fs_report['examples'][:log_examples]:
            search_output += example

        util_handle_report_output(search_output, "search_report.txt", OVERWRITE_REPORTS, output_path)

    #####################
    # KEY SEARCH REPORT #
    #####################
    if key_search:
        keyt_search_output = ""
        keyt_search_output += f"\n{'*' * 28}\n* Attribute Search Results *\n{'*' * 28}\n\n"
        keyt_search_output += f"Showing {max_print} larges values for \"{key_search}\" attribute" \
                              f" grouped by query shape.\n\n"
        for i in enumerate(sorted(key_search_report, key=lambda x: x['value'], reverse=True)[:max_print]):
            keyt_search_output += f"Key search Result {i[0] + 1}:\n"
            for k, v in i[1].items():
                keyt_search_output += f"\t{k}: {v}\n"
        util_handle_report_output(keyt_search_output, f"{key_search}_report.txt", OVERWRITE_REPORTS, output_path)

    ################
    # RATIO REPORT #
    ################
    if ratio:
        ratio_output = ""
        ratio_output += f"\n{'*' * 26}\n* Ratio Analysis Results *\n{'*' * 26}\n\n"
        ratio_output += f"Showing {max_print} worst ratios grouped by query shape.\n\n"
        for i in enumerate(sorted(ratio_report, key=lambda x: x['Ratio'], reverse=True)[:max_print]):
            ratio_output += f"Ratio Result {i[0] + 1}:\n"
            for k, v in i[1].items():
                ratio_output += f"\t{k}: {v}\n"
        util_handle_report_output(ratio_output, "ratio_report.txt", OVERWRITE_REPORTS, output_path)


def parser_search_terms(search_terms, line, rep, line_json, ):
    return
    # for term in search_terms:
    #     if term in line:
    #         # Add line
    #         rep[term]['examples'].append(line)
    #
    #         # Add namespace
    #         ns = util_get_operation_namespace(line_json)
    #         if rep[term]['ns'].get(ns):
    #             rep[term]['ns'][ns] += 1
    #         else:
    #             rep[term]['ns'][ns] = 1
    #
    #         # Filter Analysis
    #         query_details = util_get_query_details(line_json, ns, line)
    #
    #         if query_details:
    #             found = 0
    #             for qd in rep[term]['query_shapes']:
    #                 if qd['filter'] == query_details['filter'] \
    #                         and qd['type'] == query_details['type']:
    #                     qd['count'] += 1
    #                     found = 1
    #             if not found:
    #                 query_details['count'] = 1
    #                 rep[term]['query_shapes'].append(query_details)


def parser_full_search(line: str, line_json: dict, qout: Queue):
    example = line.replace('\n', '')
    ns = util_get_operation_namespace(line_json)
    query_details = util_get_query_details(line_json, ns, line)
    result = dict(example=example, ns=ns, query_details=query_details)

    qout.put(dict(report="full_search_report", result=result))


def parser_ratio(line: str, line_json: dict, qout: Queue, ratio_threshold: int = 2):
    ns = util_get_operation_namespace(line_json)
    operation_detail = util_get_query_details(line_json, ns, line)
    shape = operation_detail['filter'] if operation_detail else None
    scanned = line_json['attr'].get('docsExamined')
    returned = line_json['attr'].get('nreturned')
    plan = line_json['attr'].get('planSummary')
    keys = line_json.get('keysExamined')

    if scanned and returned:
        ratio = float(scanned) / returned
    else:
        ratio = None
    if ratio and ratio > ratio_threshold:
        result = {
            'Count': 1,
            'Ratio': int(ratio),
            "query_shape": shape,
            "ns": ns,
            "docsExamined": scanned,
            "nreturned": returned,
            "keysExamined": keys,
            "planSummary": plan,
            "example": line
        }
        qout.put(dict(report="ratio_report", result=result))


def parser_key_search(line_json, line, key, qout: Queue):
    ns = util_get_operation_namespace(line_json)
    operation_detail = util_get_query_details(line_json, ns, line)
    shape = operation_detail['filter'] if operation_detail else None
    value = util_get_key_value(line_json, key)
    plan = line_json['attr'].get('planSummary')

    if value and shape:
        result = {
            'Count': 1,
            "attribute": key,
            "value": value,
            "query_shape": shape,
            "ns": ns,
            "planSummary": plan,
            "example": line
        }
        qout.put(dict(report="key_search_report", result=result))


def util_filename_add_ts(fn: str, prefix: bool = True):
    if prefix:
        return f"{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}_{fn}"
    else:
        return f"{fn.rsplit('.', 1)[0]}_{datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')}.{fn.rsplit('.', 1)[1]}"


def util_get_key_value(d: dict, t: str):
    for k, v in d.items():
        if k == t:
            return v
        elif isinstance(v, dict):
            r = util_get_key_value(v, t)
            if r:
                return r
    return None


def util_get_query_details(line_json, ns, line):
    query_details = None
    if line_json.get("attr") and line_json["attr"].get('type'):

        qtype = line_json["attr"]['type']
        qplan = line_json['attr'].get('planSummary')
        docs_inserted = line_json["attr"].get("ninserted")
        keys_inserted = line_json["attr"].get("keysInserted")
        write_conflicts = line_json["attr"].get("writeConflicts")

        qfilter = None
        qsort = None
        update_shape = None
        op_type = None

        # COMMAND
        if qtype == "command":
            op_type = list(line_json.get("attr").get("command").keys())[0]

            if op_type == "find":
                qfilter = line_json["attr"]["command"]["filter"]
                qsort = line_json['attr']['command'].get('sort')
            elif op_type == "getMore":
                qfilter = line_json["attr"]["originatingCommand"].get('filter')
                qsort = line_json['attr']['originatingCommand'].get('sort')
            elif op_type == "aggregate":
                qfilter = line_json["attr"]["command"]["pipeline"]
            elif op_type == "distinct":
                qfilter = line_json["attr"]["command"]["query"]
            elif op_type in ("insert", "update"):
                pass
            elif op_type == "delete":
                pass
            elif op_type in ["hello", "serverStatus", "ismaster", "saslStart", "isMaster",
                             "_refreshQueryAnalyzerConfiguration", "ping", "replSetHeartbeat",
                             "replSetUpdatePosition", "saslContinue", "collStats"]:
                pass
            else:
                print(f"\n{line}\n")
                raise NotImplementedError(f"Command type \"{op_type}\" is not parsed!")

        # UPDATE
        elif qtype == "update":
            if line_json["attr"].get('command') and line_json["attr"]['command'].get('q'):
                qfilter = line_json["attr"]["command"].get("q")
                qsort = line_json['attr']['command'].get('sort')
                qupdate = line_json['attr']['command'].get('u')
                update_shape = devalue_json(qupdate)

        # Remove
        elif qtype == "remove":
            if line_json["attr"].get('command') and line_json["attr"]['command'].get('q'):
                qfilter = line_json["attr"]["command"].get("q")

        # other
        else:
            print(f"\rOperation type \"{qtype}\" not parsed!\n{line}\n")
            raise NotImplementedError(f"\rOperation type \"{qtype}\" not parsed!\n{line}\n")

        if qfilter:
            filter_shape = devalue_json(qfilter)
            query_details = dict()
            query_details['type'] = f"{qtype}({op_type})" if op_type else qtype
            query_details['ns'] = ns
            query_details['filter'] = filter_shape
            if qsort:
                query_details['sort'] = qsort
            if update_shape:
                query_details['update'] = update_shape
            if qplan:
                query_details["planSummary"] = qplan
            if docs_inserted:
                query_details['ninserted'] = qsort
            if keys_inserted:
                query_details['keysInserted'] = qsort
            if write_conflicts:
                query_details['writeConflicts'] = write_conflicts
    else:
        pass

    return query_details


def util_get_operation_namespace(line_json):
    if line_json.get("attr") \
            and line_json['attr'].get('ns'):
        ns = line_json.get("attr").get("ns")
    else:
        ns = "N/A"
    if "$cmd" in ns:
        op_coll = list(line_json.get("attr").get("command").values())[0]
        op_db = line_json.get("attr").get("command").get('$db')
        ns = f"{op_db}.{op_coll}"
    return ns


def util_handle_report_output(report_output, report_name, overwrite=False, output_path=None, analyzer_params=None):
    if output_path:
        if analyzer_params:
            report_output = analyzer_params + "\n" + report_output
        if overwrite:
            fpath = os.path.join(output_path, report_name)
        else:
            fpath = os.path.join(output_path, util_filename_add_ts(report_name))
        with open(fpath, "w") as f:
            f.write(report_output)
        print(f"Results saved to {fpath}")
    else:
        print(report_output)


def util_update_time_stamps(min_ts, max_ts, ts):
    if not min_ts or date_from_string(ts) < date_from_string(min_ts):
        min_ts = ts
    if not max_ts or date_from_string(ts) > date_from_string(max_ts):
        max_ts = ts
    return min_ts, max_ts


def task_generate_queue(qin: Queue, qin_done: Event, lines: list):
    i = 0
    print(f"Reading log")
    for line in lines:
        print_progress_bar(i + 1, len(lines), length=30)
        qin.put(line)
        i += 1
    qin_done.set()


def task_generate_reports(qout: Queue, qout_done: Event, reports_done: Event, reports_failed: Event,
                          ratio_report: list, key_search_report: list, full_search_report: dict):
    try:
        while not reports_done.is_set():
            if qout.empty():
                if qout_done.is_set():
                    reports_done.set()
                else:
                    time.sleep(0.1)
            else:
                item = qout.get()
                report = item.get('report')
                result = item.get('result')

                if report == "search_terms_report":
                    pass

                elif report == "full_search_report":
                    # Add namespace
                    ns = result.get('ns')
                    if full_search_report['ns'].get(ns):
                        full_search_report['ns'][ns] += 1
                    else:
                        full_search_report['ns'][ns] = 1

                    # Add Filter
                    query_details = result.get('query_details')

                    if query_details:
                        found = 0
                        for qd in full_search_report['query_shapes']:
                            if qd['filter'] == query_details['filter'] \
                                    and qd['type'] == query_details['type'] \
                                    and qd['ns'] == query_details['ns']:
                                qd['Count'] += 1
                                found = 1
                        if not found:
                            query_details['Count'] = 1
                            full_search_report['query_shapes'].append(query_details)

                    # Example
                    full_search_report['examples'].append(result['example'])

                elif report == "key_search_report":
                    found = False
                    if isinstance(result['value'], int):
                        for i in key_search_report:
                            if i['query_shape'] == result['query_shape']:
                                found = True
                                if i['value'] < result['value']:
                                    i['value'] = result['value']
                                    i['planSummary'] = result['planSummary']
                                    i['example'] = result['example']
                                i['Count'] += 1
                    else:
                        for i in key_search_report:
                            if i['query_shape'] == result['query_shape'] and i['value'] == result['value']:
                                i['Count'] += 1
                                found = True
                    if not found:
                        result['Count'] = 1
                        key_search_report.append(result)

                elif report == "ratio_report":
                    found = False
                    for i in ratio_report:
                        if i['query_shape'] == result['query_shape']:
                            found = True
                            if i['Ratio'] < result['Ratio']:
                                i['Ratio'] = result['Ratio']
                                i['planSummary'] = result['planSummary']
                                i['docsExamined'] = result['docsExamined']
                                i["nreturned"] = result["nreturned"]
                                i["keysExamined"] = result["keysExamined"]
                                i['example'] = result['example']
                            i['Count'] += 1
                            break
                    if not found:
                        result['Count'] = 1
                        ratio_report.append(result)

                else:
                    raise NotImplementedError(f"Unexpected Report key: \"{report}\".")
    except Exception as err:
        reports_failed.set()
        raise err


def task_parse_log(qin: Queue, qout: Queue, qout_done: Event, qin_done: Event,
                   path, err_limit, start_time, end_time,
                   ratio,
                   key_search,
                   full_search_include, full_search_exclude, ):
    json_decoder_errors_counter = 0

    while not qout_done.is_set():
        if qin.empty():
            if qin_done.is_set():
                qout_done.set()
            else:
                time.sleep(0.1)
        else:
            line = qin.get()
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

            # ################
            # # SEARCH TERMS #
            # ################
            # if search_terms:
            #     if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
            #             and any(term in line for term in search_terms):
            #
            #         t = Thread(target=task_search_term_analysis,
            #                    args=(search_terms, line, search_terms_report, line_json),
            #                    daemon=True)
            #         t.start()
            #         while not len(st_threads) < 100:
            #             for t in st_threads:
            #                 t.join()
            #                 st_threads.remove(t)
            #         st_threads.append(t)
            #         # search_term_analysis(search_terms, line, search_terms_report, line_json)

            ###############
            # FULL SEARCH #
            ###############
            if len(full_search_include) > 0 or len(full_search_exclude) > 0:
                if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time):
                    match = True
                    for term in full_search_include:
                        if term not in line:
                            match = False
                            break
                    if match:
                        for term in full_search_exclude:
                            if term in line:
                                match = False
                                break
                    if match:

                        t = Thread(target=parser_full_search,
                                   args=(line, line_json, qout, ),
                                   daemon=True)
                        t.start()
                        work_threads.append(t)
                        # parser_full_search(line, line_json, qout, )

            #########
            # RATIO #
            #########
            if ratio:
                if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                        and "docsExamined" in line and "nreturned" in line:
                    t = Thread(target=parser_ratio,
                               args=(line, line_json, qout,),
                               daemon=True)
                    t.start()
                    work_threads.append(t)
                    # parser_ratio(line, line_json, qout, )

            ##############
            # Key Search #
            ##############
            if key_search:
                if date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                        and key_search in line:

                    t = Thread(target=parser_key_search,
                               args=(line_json, line, key_search, qout),
                               daemon=True)
                    t.start()
                    work_threads.append(t)
                    # parser_key_search(line_json, line, key_search, qout)

            for t in work_threads:
                t.join()


if __name__ == "__main__":
    multiprocessing.freeze_support()
    main()
