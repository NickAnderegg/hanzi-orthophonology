import pandas
import matplotlib.pyplot as plt
import matplotlib
matplotlib.style.use('ggplot')

syllables = pandas.read_csv('utilities/distances.txt', header=None, names=['s1', 's2', 'diff'])
plt.figure()
syllables.plot.hist()
