import json
import argparse
import multiprocessing
import os
import time

from random import randint
from threading import Thread
from multiprocessing import Event, Queue

from utils.obj import FormTemplate
from utils.ops import print_progress_bar, date_from_string, devalue_json

PATH = None
# PATH = "~/Downloads/sampleLog.log"
TIME_S = "1970-01-01T00:00:00"
TIME_E = "2830-01-01T00:00:00"
DECODER_ERR_MAX = 100
MAX_PRINT = 10
MAX_EXAMPLES = 3

SEARCH_TERMS = ""
SEARCH = ""
RATIO = True
DURATION = True

REPORTS_PATH = ""


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
    parser.add_argument('-pl', "--max_print", help=f"Print top N results. ex: 100", type=int,
                        required=False, default=MAX_PRINT)
    parser.add_argument('-el', "--max_examples", help=f"Print up to N examples from log. ex: 100", type=int,
                        required=False, default=MAX_EXAMPLES)
    parser.add_argument('-op', "--output_path", help=f"If provided, will save report in files at path. "
                                                     f"ex: ~/Documents/QueriesReports",
                        required=False, default=REPORTS_PATH)
    parser.add_argument('-st', "--search_terms", help=f"Comma separated search terms. ex:\"COLLSCAN,hasSortStage\"",
                        required=False, default=SEARCH_TERMS)
    parser.add_argument('-fs', "--search", help=f"Full search. All terms must be in line. Terms starting with \"-\""
                                                f" must not be in line ex:\"-COLLSCAN,hasSortStage\"",
                        required=False, default=SEARCH_TERMS)
    parser.add_argument('-ratio', "--ratio", help=f"Include ratio analysis . ex: 1/0", type=bool,
                        required=False, default=RATIO)
    parser.add_argument('-duration', "--duration", help=f"Include duration analysis . ex: 1/0", type=bool,
                        required=False, default=DURATION)
    args = parser.parse_args()

    if args.path and os.path.sep not in args.path:
        args.path = os.path.join(os.getcwd(), args.path)

    if args.path:
        analyzer(**vars(args))
    else:
        form().open()


def form():
    root = FormTemplate(500, 350)
    root.add_title("Log Analyzer")
    root.add_wellcome_message("Queries Analyzer")

    log_path = root.add_text_field(name="Log File", default="~/Downloads/mongodb.log", info="Path to log file")
    start_time = root.add_text_field(name="Start Time", default="1970-01-01T00:00:00", info="Start time")
    end_time = root.add_text_field(name="End Tme", default="2830-01-01T00:00:00", info="End time")
    decoder_error_limit = root.add_num_field(name="Decoder Error Limit", default=100, info="Max allowed decoder errors")
    max_print = root.add_num_field(name="Top N results to print", default=10, info="Max combined results to print")
    output_path = root.add_text_field(name="Output Path", default="", info="If provided, will save reports in path")

    root.add_separator()
    search_terms = root.add_text_field(name="Single Search Terms", default="",
                                       info="Comma separated terms to scan the log for. No Spaces. "
                                            "Checks log for each term separately. Leave empty to skip")
    search = root.add_text_field(name="Full Search Terms",
                                 default="",
                                 info="Comma separated search words to look for in log. No Spaces. Checks the log for"
                                      "all search terms to be present. Leave empty to skip"
                                      "Terms starting with \"-\" must not be in line")
    max_examples = root.add_num_field(name="Log examples to print", default=1, info="Max log examples to print")

    ratio = root.add_bool_field(name="Do Ratio Analysis", default=False, info="Include Ratio Analysis")
    duration = root.add_bool_field(name="Do Slow Ops Analysis", default=False,
                                   info="Show slowest running ops grouped by query shape")

    root.set_action_button(
        "Run Analyzer",
        lambda: analyzer(
            path=log_path.get(),
            start_time=start_time.get(),
            end_time=end_time.get(),
            error_limit=decoder_error_limit.get(),
            max_print=max_print.get(),
            output_path=output_path.get(),
            search_terms=search_terms.get(),
            search=search.get(),
            max_examples=max_examples.get(),
            ratio=ratio.get(),
            duration=duration.get(),
            # include=include.get()
        )
    )

    return root


def analyzer(*args, **kwargs):
    print(f"\n{'-' * 3}\n\nAnalyzer Parameters:")
    for arg in args:
        print(f"\t{arg}")
    for k, v in kwargs.items():
        print(f"\t{k}:{(17 - len(k)) * ' '}{v}")
    print("\n")

    kwargs['path'] = kwargs['path'].replace('~', os.path.expanduser('~'))

    search_terms = kwargs['search_terms'].split(",") if kwargs['search_terms'] else None

    search = kwargs['search'].split(",") if kwargs.get('search') and kwargs['search'] else None

    analyzer_main_task(
        path=kwargs['path'],
        err_limit=kwargs['error_limit'],
        start_time=kwargs['start_time'],
        end_time=kwargs['end_time'],
        max_print=kwargs['max_print'],
        max_examples=kwargs.get('max_examples'),
        search_terms=search_terms,
        ratio=kwargs['ratio'],
        ratio_threshold=kwargs.get('ratio_threshold') or 1000,
        duration=kwargs.get('duration'),
        search=search,
        output_path=kwargs.get('output_path'),
    )


def analyzer_main_task(path, start_time, end_time, search_terms: list, search: list,
                       err_limit: int = 100, max_print: int = 10, max_examples: int = 10, ratio: bool = False,
                       ratio_threshold=1000, duration: bool = True, output_path: str = None):
    try:
        with open(path) as f:
            print(f"Reading \'{path}\'")
            log_file = f.readlines()
    except FileNotFoundError:
        raise FileNotFoundError(f"No such file: \'{path}\'")

    min_ts = None
    max_ts = None
    thread_lists = []

    # Search Terms Report
    search_terms_report = dict()
    if search_terms:
        for term in search_terms:
            search_terms_report[term] = {
                "examples": list(),
                "ns": dict(),
                'query_shapes': list()
            }
    st_threads = []
    thread_lists.append(st_threads)

    # Search Report
    fs_report = {
        "examples": list(),
        "ns": dict(),
        'query_shapes': list()
    }
    fs_threads = []
    thread_lists.append(fs_threads)
    search_include, search_exclude = list(), list()
    if search:
        for st in search:
            if st.startswith("-"):
                search_exclude.append(st.replace('-', ''))
            else:
                search_include.append(st)

    # Ratio Report
    ratio_report = list()
    ratio_threads = []
    thread_lists.append(ratio_threads)

    # Duration Report
    duration_report = list()
    duration_threads = []
    thread_lists.append(duration_threads)

    # Process Line
    exec_start = time.time()

    json_decoder_errors_counter = 0
    print(f"Parsing log")
    for eline in enumerate(log_file):
        i, line = eline
        print_progress_bar(i + 1, len(log_file), length=30,
                           suffix=f"Report Threads: Terms({len(st_threads)}), "
                                  f"Ratio({len(ratio_threads)}), "
                                  f"SlowOps({len(duration_threads)}) "
                                  f"Search({len(fs_threads)})")

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
        min_ts, max_ts = util_update_time_stamps(min_ts, max_ts, time_stamp)

        ################
        # SEARCH TERMS #
        ################
        if search_terms \
                and date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                and any(term in line for term in search_terms):

            t = Thread(target=task_search_term_analysis, args=(search_terms, line, search_terms_report, line_json))
            t.start()
            while not len(st_threads) < 100:
                for t in st_threads:
                    t.join()
                    st_threads.remove(t)
            st_threads.append(t)
            # search_term_analysis(search_terms, line, search_terms_report, line_json)

        ##########
        # SEARCH #
        ##########
        if (len(search_include) > 0 or len(search_exclude) > 0) \
                and date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time):
            match = True
            for term in search_include:
                if term not in line:
                    match = False
                    break
            if match:
                for term in search_exclude:
                    if term in line:
                        match = False
                        break
            if match:

                t = Thread(target=task_search_analysis,
                           args=(search_include, search_exclude, line, line_json, fs_report,),
                           daemon=True)
                t.start()
                while len(fs_threads) >= 100:
                    for t in fs_threads:
                        t.join()
                        fs_threads.remove(t)
                fs_threads.append(t)
                # task_search_analysis(search_include, search_exclude, line, line_json, fs_report, )

        #########
        # RATIO #
        #########
        if ratio and date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                and "docsExamined" in line and "nreturned" in line:

            t = Thread(target=task_ratio_analysis, args=(ratio_report, line, line_json, ratio_threshold))
            t.start()
            while not len(ratio_threads) < 100:
                for t in ratio_threads:
                    t.join()
                    ratio_threads.remove(t)
            ratio_threads.append(t)
            # ratio_analysis(ratio_report, line, line_json)

        ############
        # DURATION #
        ############
        if duration and date_from_string(end_time) > date_from_string(time_stamp) > date_from_string(start_time) \
                and "durationMillis" in line:

            t = Thread(target=task_duration_analysis, args=(duration_report, line, line_json))
            t.start()
            while not len(duration_threads) < 100:
                for t in duration_threads:
                    t.join()
                    duration_threads.remove(t)
            duration_threads.append(t)
            # task_duration_analysis(duration_report, line, line_json)

        # /Process Log Line

    # Join them worker bees
    for tl in thread_lists:
        for t in tl:
            t.join(10)
    exec_duration = round(time.time() - exec_start, 2)

    ##########
    # OUTPUT #
    ##########
    print(f"\nAnalysis Done in {exec_duration} seconds. Printing results from {min_ts} to {max_ts}\n")

    ######################
    # Search Term report #
    ######################

    if search_terms:
        search_terms_output = ""
        search_terms_output += f"\n{'*' * 23}\n* Search Terms Report *\n{'*' * 23}\n"

        for k, v in search_terms_report.items():
            search_terms_output += f"\nSearch Term \"{k}\" yielded {len(v['examples'])} results."
            # Namespaces
            search_terms_output += f"\n\n\tTop {max_print} NameSpaces (out of {len(v['ns'])}):"
            for ns, count in dict(sorted(v['ns'].items(), key=lambda x: x[1], reverse=True)[:max_print]).items():
                search_terms_output += f"\n\t\t{ns}: {count}"
            # Query Shapes
            search_terms_output += f"\n\n\tTop {max_print} Query Shapes (out of {len(v['query_shapes'])}):"
            for fs in sorted(v['query_shapes'], key=lambda x: x['count'], reverse=True)[:max_print]:
                search_terms_output += f"\n\t\t{json.dumps(fs)}"
            # Example Queries
            if max_examples:
                search_terms_output += f"\n\n\tExamples ({len(v['examples'])}):"
                for example in v['examples'][:max_examples]:
                    search_terms_output += f"\n" + example.replace('\n', '')
            search_terms_output += f"\n\n{'-' * 50}\n"

        print(search_terms_output)
        dump_to_file(output_path, "searchTermsReport.txt", search_terms_output)

    #################
    # SEARCH REPORT #
    #################
    if search:
        search_output = ""
        search_output += f"\n{'*' * 18}\n* Search Results *\n{'*' * 18}\n\n"
        search_output += f"Search: {search}\n"
        search_output += f"\n\tTop {max_print} NameSpaces (out of {len(fs_report['ns'])}):\n"
        for ns, count in dict(sorted(fs_report['ns'].items(), key=lambda x: x[1], reverse=True)[:max_print]).items():
            search_output += f"\t\t{ns}: {count}\n"
        search_output += f"\n\tTop {max_print} Query Shapes (out of {len(fs_report['query_shapes'])}):\n"
        for qs in sorted(fs_report['query_shapes'], key=lambda x: x['count'], reverse=True)[:max_print]:
            for k, v in qs.items():
                search_output += f"\t\t{k}: {v}\n"
            search_output += "\n"
        if max_examples:
            search_output += f"\n\tExamples ({len(fs_report['examples'])}):\n"
            for example in fs_report['examples'][:max_examples]:
                search_output += example  # .replace("\n", '')

        print(search_output)
        dump_to_file(output_path, "searchReport.txt", search_output)
        print(f"\n{'-' * 50}\n")

    ################
    # RATIO REPORT #
    ################
    if ratio:
        ratio_output = ""
        ratio_output += f"\n{'*' * 16}\n* Ratio Report *\n{'*' * 16}\n\n"
        ratio_output += f"Showing {max_print} worst ratios grouped by query shape.\n"
        for i in enumerate(sorted(ratio_report, key=lambda x: x['Ratio'], reverse=True)[:max_print]):
            ratio_output += f"\nNumber {i[0]+1}.\n"
            for k, v in i[1].items():
                ratio_output += f"\t{k}: {v}\n"
        print(ratio_output)
        dump_to_file(output_path, "ratiosReport.txt", ratio_output)
        print(f"\n{'-' * 50}\n")

    ###################
    # Duration Report #
    ###################
    if duration:
        duration_output = ""
        duration_output += f"\n{'*' * 19}\n* Duration Report *\n{'*' * 19}\n\n"
        duration_output += f"Showing {max_print} longest running query shapes.\n"
        for i in enumerate(sorted(duration_report, key=lambda x: x['durationMillis'], reverse=True)[:max_print]):
            duration_output += f"\nNumber {i[0]+1}.\n"
            for k, v in i[1].items():
                duration_output += f"\t{k}: {v}\n"
        print(duration_output)
        dump_to_file(output_path, "durationReport.txt", duration_output)
        print(f"\n{'-' * 50}\n")


def task_search_term_analysis(search_terms, line, rep, line_json, ):
    # Process Log Line
    for term in search_terms:
        if term in line:
            # Add line
            rep[term]['examples'].append(line)

            # Add namespace
            ns = util_get_operation_namespace(line_json)
            if rep[term]['ns'].get(ns):
                rep[term]['ns'][ns] += 1
            else:
                rep[term]['ns'][ns] = 1

            # Filter Analysis
            query_details = util_get_query_details(line_json, ns, line)

            if query_details:
                found = 0
                for qd in rep[term]['query_shapes']:
                    if qd['filter'] == query_details['filter'] \
                            and qd['type'] == query_details['type']:
                        if qd.get('writeConflicts') and query_details.get('writeConflicts'):
                            qd['writeConflicts'] = max(query_details['writeConflicts'], qd['writeConflicts'])
                        qd['count'] += 1
                        found = 1
                if not found:
                    query_details['count'] = 1
                    rep[term]['query_shapes'].append(query_details)


def task_search_analysis(include: list, exclude: list, line: str, line_json: dict, report: dict):
    # Add line
    report['examples'].append(line.replace('\n', ''))

    # Add namespace
    ns = util_get_operation_namespace(line_json)
    if report['ns'].get(ns):
        report['ns'][ns] += 1
    else:
        report['ns'][ns] = 1

    # Filter Analysis
    query_details = util_get_query_details(line_json, ns, line)

    if query_details:
        found = 0
        for qd in report['query_shapes']:
            if qd['filter'] == query_details['filter'] \
                    and qd['type'] == query_details['type'] \
                    and qd['ns'] == query_details['ns']:
                qd['count'] += 1
                if qd.get('writeConflicts') and query_details.get('writeConflicts'):
                    qd['writeConflicts'] = max(query_details['writeConflicts'], qd['writeConflicts'])
                found = 1
        if not found:
            query_details['count'] = 1
            report['query_shapes'].append(query_details)


def task_ratio_analysis(ratio_report: list, line: str, line_json: dict, ratio_threshold: int):
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
        found = False
        for i in ratio_report:
            if i['query_shape'] == shape:
                found = True
                if i['Ratio'] < int(ratio):
                    i['Ratio'] = int(ratio)
                    i['planSummary'] = plan
                    i['docsExamined'] = scanned
                    i["nreturned"] = returned
                    i["keysExamined"] = keys
                    i['example'] = line.replace('\n', '')
                i['count'] += 1
        if not found:
            ratio_report.append(
                {
                    "count": 1,
                    'Ratio': int(ratio),
                    "query_shape": shape,
                    "ns": ns,
                    "docsExamined": scanned,
                    "nreturned": returned,
                    "keysExamined": keys,
                    "planSummary": plan,
                    "example": line.replace('\n', '')
                }
            )


def task_duration_analysis(duration_report: list, line: str, line_json: dict):
    ns = util_get_operation_namespace(line_json)
    operation_detail = util_get_query_details(line_json, ns, line)
    shape = operation_detail['filter'] if operation_detail else None
    duration = line_json['attr'].get('durationMillis')
    plan = line_json['attr'].get('planSummary')

    if duration and shape:
        found = False
        for i in duration_report:
            if i['query_shape'] == shape:
                found = True
                if i['durationMillis'] < duration:
                    i['durationMillis'] = duration
                    i['planSummary'] = plan
                    i['example'] = line.replace('\n', '')
                i['count'] += 1
        if not found:
            duration_report.append(
                {
                    "count": 1,
                    "durationMillis": duration,
                    "query_shape": shape,
                    "ns": ns,
                    "planSummary": plan,
                    "example": line.replace('\n', '')
                }
            )


def dump_to_file(path: str, filename: str, report: str):
    if path:
        path = path.replace('~', os.path.expanduser('~'))
        if not os.path.exists(path):
            os.makedirs(path)

        file_path = os.path.join(path, filename)

        report_file = open(file_path, 'w')
        report_file.write(report)
        report_file.flush()
        report_file.close()


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
                             "replSetUpdatePosition", "saslContinue"]:
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


def util_update_time_stamps(min_ts, max_ts, ts):
    if not min_ts or date_from_string(ts) < date_from_string(min_ts):
        min_ts = ts
    if not max_ts or date_from_string(ts) > date_from_string(max_ts):
        max_ts = ts
    return min_ts, max_ts


if __name__ == "__main__":
    main()
