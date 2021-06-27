import pandas as pd
import numpy as np
from sklearn.cluster import KMeans
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.cluster import MeanShift, estimate_bandwidth
import matplotlib.pyplot as plt
from typing import Iterable
import sys

sys.path.insert(1, "C:/Users/Eddie/Documents/language-change-methods")
from word_clouds import make_wordcloud
from features import get_ngram_lr_and_ll

colour_list = ["#66c2a5", "#fc8d62", "#8da0cb", "#e78ac3", "#a6d854"]


def scale_feats(feats: np.ndarray):
    scaler = MinMaxScaler()
    scaled = scaler.fit_transform(feats)
    return scaled


def k_means_cluster(feats: np.ndarray, n_clusts: int, random_state: int):
    km = KMeans(n_clusters=3, random_state=1234)
    km = km.fit(feats)
    km_clusts = km.predict(feats)
    km_centres = km.cluster_centers_
    return km_clusts, km_centres


def log_and_scale(feats: np.ndarray, scaled: bool, logged: bool):
    # Log the features if desired
    if logged:
        feats = np.log2(feats)
    # Scale the features if required
    if scaled:
        feats = scale_feats(feats)
    return feats


def do_kmeans_clustering(feats: np.ndarray, n_clusts: int, random_state: int, scaled: bool, logged: bool):
    # log and scale the features if required
    feats = log_and_scale(feats, scaled, logged)
    # Perform the k means clustering
    return k_means_cluster(feats, n_clusts=n_clusts, random_state=random_state)


def plot_clusters(data: np.ndarray, clusts: np.ndarray, centres: dict = None):
    fig, ax = plt.subplots(figsize=(10, 6))

    # make a quick dictionary to refer to when allocating colours.
    colours = {clust: colour_list[i] for i, clust in enumerate(set(clusts))}

    for clust in set(clusts):
        curr = data[clusts==clust]
        ax.scatter(curr[:,0], curr[:,1], color=colours[clust], alpha=0.2, label=clust)
        ax.scatter(centres[clust][0], centres[clust][1], color=colours[clust], s=1000, alpha=1, marker="x", zorder=100)

    ax.legend()

    return fig, ax


def get_inter_group_kw(group_dic, toks, n=1, join_char="_"):
    """
    Gets the keywords between every combination of groups.
    @param group_list: an iterable containing pandas Series made up of post indices for each group.
    """
    # Initialise an empty dictionary for the keywords
    combi_kw = {g: dict() for g in group_dic.keys()}
    # Loop through each combination.
    for g1_name, g1 in group_dic.items():
        g1_toks = toks[toks.index.isin(g1)]
        for g2_name, g2 in group_dic.items():
            g2_toks = toks[toks.index.isin(g2)]
            if g1_name != g2_name:
                combi_kw[g1_name][g2_name] = get_ngram_lr_and_ll(g1_toks, g2_toks, n=n, join_char=join_char)
            
    return combi_kw


def display_group_kw_combis(group_kw):
    gnames = list(group_kw.keys())
    num_groups = len(group_kw)

    fig = plt.figure(figsize=(50, 50))
    gs = fig.add_gridspec(num_groups, num_groups, hspace=0, wspace=0)
    axes = gs.subplots(sharex=True, sharey=True)
    
    for i, g1 in enumerate(gnames):
        for j, g2 in enumerate(gnames):
            if g2 in group_kw[g1]:
                curr_kw = group_kw[g1][g2]
                if len(curr_kw) > 0:
                    cloud = make_wordcloud(curr_kw)
                    axes[i, j].imshow(cloud, aspect="auto")
            else:
                pass
            
            axes[i, j].axes.xaxis.set_ticks([])
            axes[i, j].axes.yaxis.set_ticks([])
            
        
    # Add the y labels
    for i, curr_ax in enumerate(axes[:, 0]):
        curr_ax.set_ylabel(gnames[i], fontsize=60, rotation=90)
    
    # Add the x labels
    for i, curr_ax in enumerate(axes[-1, :]):
        curr_ax.set_xlabel(gnames[i], fontsize=60)
        
    # plt.tight_layout(pad = 10)
    return fig


def plot_ace():
    pass
    