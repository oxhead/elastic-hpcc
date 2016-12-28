def generate_file(file_template, file_output, token_old, token_new):
    with open(file_template) as f_in:
        with open(file_output, 'w') as f_out:
            for line in f_in:
                f_out.write(line.replace(token_old, token_new))


def main(num):
    for i in range(1, num+1):
        generate_file('SequentialSearch_FirstName_template.ecl', 'SequentialSearch_FirstName_{}.ecl'.format(i), '{id}', str(i))


if __name__ == '__main__':
    import sys
    main(int(sys.argv[1]))