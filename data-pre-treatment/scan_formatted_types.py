import re
from concurrent.futures.thread import ThreadPoolExecutor

import boto3


def scan_data_types():
    bucket = "taxi-poc-formatted"

    s3 = boto3.client("s3")
    file_names = s3.list_objects(Bucket=bucket)["Contents"]
    file_names = [x["Key"] for x in file_names if x["Size"] > 0]

    def get_file_header(file_name):
        for header in s3.get_object(Bucket=bucket, Key=file_name)["Body"].iter_lines():
            header = "".join(re.findall("[a-zA-Z,_]+", header.decode("utf8"))).lower()
            return {"name": file_name, "header": header}

    with ThreadPoolExecutor(max_workers=32) as executor:
        file_list = list(executor.map(get_file_header, file_names))

    with open("resource/formatted_file_headers.txt", "w") as f:
        file_headers = sorted(list(set(x["header"] for x in file_list)))
        f.writelines(f"{x}\n" for x in file_headers)
        f.write("---------------------\n")
        file_list = [(re.findall(r"\d{4}-\d{2}-\d{2}", x["name"])[-1],
                      x["name"], str(file_headers.index(x["header"]))) for x in file_list]
        f.writelines(f"{','.join(x)}\n" for x in sorted(file_list))


if __name__ == '__main__':
    scan_data_types()
