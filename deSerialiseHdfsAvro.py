import hdfs
import fastavro
import sys

# Lecture de hdfs sous forme avro
def lec(adresseHdfs, repDataHdfs):
    schema = {
        "namespace": "ffo.hashtag",
        "type": "record",
        "name": "Node",
        "fields": [
            {"name": "datehashtag", "type": "string"},
            {"name": "timestamp", "type": "int"},
            {"name": "hashtags", "type": {"type": "array", "items": "string"}, "default": {}},
        ]
    }

    hdfs_client = hdfs.InsecureClient(adresseHdfs, schema)
    with hdfs_client.read(repDataHdfs) as of:
        reader = fastavro.reader(of)
        for node in reader:
            print(node)

def main():
    # Read command line arguments
    adresseHdfs = sys.argv[1]
    repDataHdfs = sys.argv[2]

    # Lecture de hdfs sous forme avro
    lec(adresseHdfs, repDataHdfs)


if __name__ == "__main__":
    main()

