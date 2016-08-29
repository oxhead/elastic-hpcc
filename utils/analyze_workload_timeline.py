import pickle

timeline_file = "/home/chsu6/elastic-hpcc/mybenchmark/E15/.workload_timeline/3363d7eeb34473418439dd8b210c1f88"

with open(timeline_file, 'rb') as f:
    tl = pickle.load(f)

for t, items in tl.timeline.items():
    for item in items:
        print(t, item.wid)