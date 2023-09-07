from datetime import datetime

DATE_FORMAT = "%Y-%m-%dT%H:%M:%S"


def date_from_string(date_str, date_format=DATE_FORMAT) -> datetime:
    return datetime.strptime(date_str, date_format)


def date_to_string(date_obj: datetime, date_format=DATE_FORMAT) -> str:
    return datetime.strftime(date_obj, date_format)


def devalue_json(valued_json):
    if not valued_json:
        return valued_json
    filter_shape = None
    if isinstance(valued_json, dict):
        filter_shape = dict()
        for k, v in valued_json.items():
            if isinstance(v, dict) or isinstance(v, list):
                filter_shape[k] = devalue_json(v)
            else:
                filter_shape[k] = '###'
    elif isinstance(valued_json, list):
        if len(valued_json) > 0:
            filter_shape = list()
            if not isinstance(valued_json[0], dict):
                filter_shape = ['###']
            else:
                for i in valued_json:
                    filter_shape.append(devalue_json(i))
        else:
            filter_shape = []
    return filter_shape


def print_progress_bar(iteration, total, prefix="Progress", suffix="Complete", decimals=1, length=100, fill='█'):
    percent = round(100 * (iteration / float(total)), 2)
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    print(end=f'\r{prefix} |{bar}| {percent}% {suffix}')
    # Print New Line on Complete
    if iteration == total:
        print()
