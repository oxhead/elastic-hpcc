def main(input_file, output_file, num_keys):
    print("input:", input_file)
    print("output:", output_file)
    print("# of keys:", num_keys)
    with open(input_file, 'r') as f:
        input_keys = f.read().splitlines()
    key_distance = len(input_keys) / num_keys
    output_keys = [input_keys[i*key_distance] for i in range(num_keys)]
    with open(output_file, 'w') as f:
        for key in output_keys:
            f.write("{}\n".format(key))

if __name__ == "__main__":
    key_list = ['firstname', 'lastname', 'city', 'zip']
    num_keys = 100
    for key in key_list:
        input_file = "/home/chsu6/elastic-hpcc/benchmark/dataset/{}_list_2000.txt".format(key)
        output_file = "/home/chsu6/elastic-hpcc/benchmark/dataset/{}_list_{}.txt".format(key, num_keys)
        main(input_file, output_file, num_keys)
