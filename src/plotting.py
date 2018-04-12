import os
import matplotlib as mpl
import matplotlib.patches as mpatches
import matplotlib.pyplot as plt
from settings import IMG_DIR

mpl.rc('font', size=20)  # controls default text sizes
mpl.rc('axes', labelsize=20)  # fontsize of the x and y labels
mpl.rc('xtick', labelsize=20)  # fontsize of the tick labels
mpl.rc('ytick', labelsize=20)  # fontsize of the tick labels
mpl.rc('legend', fontsize=14)  # legend fontsize
mpl.rc('figure', titlesize=20)  #
mpl.rc('lines', linewidth=1.5)
mpl.rc('figure', figsize=(16, 9))


def double_plot(plots, names, xy_labels, title, xs=None):
    """
    Plot 2 disjoint series on the same figure, as long as they share an x-axis range

    Args:
        plots: list of plots (len 2)
        names: corresponding plot names
        xy_labels: 1 x axis label, then 2 y labels (1 per plot)
        title: plot title
        xs: x-axis values

    Returns:
        none - save plot
    """
    fig = plt.figure()
    ax = fig.add_subplot(111)

    line_style = "-"
    linewidth = 1.5
    ax.plot(xs, plots[0], linestyle=line_style, linewidth=linewidth, color="red") if xs is not None else ax.plot(
        plots[0])
    ax.set_ylabel(xy_labels[1])

    ax2 = fig.add_subplot(111, sharex=ax, frameon=False)
    ax2.plot(xs, plots[1], linestyle=line_style, linewidth=linewidth) if xs is not None else ax.plot(plot[0])
    ax2.yaxis.tick_right()
    ax2.yaxis.set_label_position("right")
    ax2.set_ylabel(xy_labels[2])

    plt.title(title)
    plt.xlabel(xy_labels[0])

    red_patch = mpatches.Patch(color='red', label=names[0])
    blue = mpatches.Patch(color='blue', label=names[1])
    plt.legend(handles=[red_patch, blue], loc='upper left')
    plt.tight_layout()
    ax.get_yaxis().set_major_formatter(
        mpl.ticker.FuncFormatter(lambda y, p: format(int(y), ',')))

    for (i, label) in enumerate(ax.get_xticklabels()):
        if 0 < i < len(ax.get_xticklabels()) - 1:
            label.set_visible(False)

    plt.savefig(os.path.join(IMG_DIR, title+".png"))
    plt.show()
    plt.close()
