import pickle

timeline_file = "/home/chsu6/elastic-hpcc/mybenchmark/E20/.workload_timeline/e852d0a465193ea3f23a26063b5dd1f9"

with open(timeline_file, 'rb') as f:
    tl = pickle.load(f)

for t, items in tl.timeline.items():
    for item in items:
        print(t, item.wid, item.endpoint, item.query_name, item.query_key, item.key)
