import numpy as np
import getopt
import defopt
import matplotlib.pyplot as plt
import csv
import sys


def main(fname : str, id : str):

    print(fname)
    print(id)

    with open(fname,'r') as csvfile:
        plots = csv.reader(csvfile, delimiter = ',')

        for row in plots:
            if len(row) == 1:
                plot_tite = row[0]
                img = plt.figure()
            elif len(row) > 1 and not row[0]:
                variables = row[1:]
            elif len(row) > 1 and row[0]:
                data = np.array(list(filter(None, row[1:])))
                plt.plot(data.astype(np.float), label=row[0])
            elif len(row) == 0:
                plt.ylabel('Time (s)')
                plt.xlabel('Variable')
                plt.title(plot_tite)
                plt.xticks(range(len(variables)),variables)
                plt.legend()
                plt.savefig(f'{id}_{plot_tite}_times.png')
                img.clear()


if __name__ == '__main__':
    defopt.run(main)
