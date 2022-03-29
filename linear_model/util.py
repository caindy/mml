import matplotlib.pyplot as plt

plt.style.use('seaborn-poster')

def plot_result(yhat, y, hours):
    plt.figure(figsize = (50, 40))
    plt.plot(hours, y, 'b.')
    plt.plot(hours, yhat, 'r.')
    plt.xlabel('hours')
    plt.ylabel('MWh')
    plt.show()