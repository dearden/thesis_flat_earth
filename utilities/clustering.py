import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import sys
import os
import itertools
import umap
from colour import Color
from collections import Counter
sys.path.insert(1, "C:/Users/Eddie/Documents/language-change-methods")
sys.path.insert(1, "C:/Users/Eddie/Documents/language-change-application/flat-earth-forum/analysis")

from group_analysis import do_kmeans_clustering, plot_clusters, log_and_scale, colour_list
from helpers import load_posts, load_toks, load_pos, get_top_n_toks
from features import get_tok_counts, function_words, combine_counts, make_feature_matrix

from sklearn.cluster import KMeans, SpectralClustering, MeanShift, estimate_bandwidth
from sklearn.preprocessing import MinMaxScaler, StandardScaler
from sklearn.pipeline import Pipeline
from sklearn.model_selection import cross_val_predict
from sklearn import metrics


markers = ['o', 'v', 'P', 's', 'H', 'D', '*', '^']
colors = ["#e41a1c", "#377eb8", "#4daf4a", "#984ea3", "#ff7f00"]

mc_list = list(itertools.product(markers, colors))


def reduce_features(matrix, random_state=123, scaler=None):
    reducer = umap.UMAP(random_state=random_state)

    if scaler is not None:
        scaled = scaler.fit_transform(scaled)
    else:
        scaled = matrix
        
    embedding = reducer.fit_transform(scaled)
    
    return embedding


def make_elbow_plot(curr_feats, start=1, end=51, step=5, scaler=None):
    """
    Makes an elbow plot to help find k in k-means clustering.
    """
    if scaler is not None:
        scaled = scaler.fit_transform(curr_feats)
    else:
        scaled = curr_feats

    sum_squared_dists = []
    K = range(start, end, step)
    for k in K:
        km = KMeans(n_clusters=k, random_state=1234)
        km = km.fit(scaled)
        sum_squared_dists.append(km.inertia_)
        
    fig, ax = plt.subplots(figsize=(10, 6))
    ax.plot(K, sum_squared_dists, 'bx-')
    ax.set_xlabel('k')
    ax.set_ylabel('SSD')
    ax.set_xticks(K)
    ax.grid()
    plt.show()


def compare_binary_normed_feature_embeddings(curr_emb_bin, curr_emb_normed, clusters=None, alpha=0.1, c=None, out_fp=None):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,5))
    if clusters is not None:
        for clust, curr_mc in zip(set(clusters), mc_list):
            ax2.scatter(curr_emb_normed[clusters==clust][:,0], curr_emb_normed[clusters==clust][:,1], alpha=alpha, c=curr_mc[1], marker=curr_mc[0], label=clust)
            ax1.scatter(curr_emb_bin[clusters==clust][:,0], curr_emb_bin[clusters==clust][:,1], alpha=alpha, c=curr_mc[1], marker=curr_mc[0], label=clust)

        ax2.legend()
        ax1.legend()

    else:
        ax2.scatter(curr_emb_normed[:,0], curr_emb_normed[:,1], alpha=alpha, c=c)
        ax1.scatter(curr_emb_bin[:,0], curr_emb_bin[:,1], alpha=alpha, c=c)

    ax2.axes.xaxis.set_ticks([])
    ax2.axes.yaxis.set_ticks([])
    ax2.set_title("Normalised Feature Counts", fontsize=14)

    ax1.axes.xaxis.set_ticks([])
    ax1.axes.yaxis.set_ticks([])
    ax1.set_title("Binary Feature Counts", fontsize=14)

    if out_fp is not None:
        fig.savefig(out_fp)

    plt.show()


def plot_bin_and_norm_clusters(curr_emb_bin, curr_emb_normed, clusters_bin, clusters_norm, alpha=0.1, c=None, out_fp=None):
    fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(12,5))
    for clust, curr_mc in zip(set(clusters_bin), mc_list):
        ax1.scatter(curr_emb_bin[clusters_bin==clust][:,0], curr_emb_bin[clusters_bin==clust][:,1], alpha=alpha, c=curr_mc[1], marker=curr_mc[0], label=clust)
    for clust, curr_mc in zip(set(clusters_norm), mc_list):    
        ax2.scatter(curr_emb_normed[clusters_norm==clust][:,0], curr_emb_normed[clusters_norm==clust][:,1], alpha=alpha, c=curr_mc[1], marker=curr_mc[0], label=clust)

    ax1.legend()
    ax2.legend()

    ax1.axes.xaxis.set_ticks([])
    ax1.axes.yaxis.set_ticks([])
    ax1.set_title("Binary Feature Counts", fontsize=14)

    ax2.axes.xaxis.set_ticks([])
    ax2.axes.yaxis.set_ticks([])
    ax2.set_title("Normalised Feature Counts", fontsize=14)

    if out_fp is not None:
        fig.savefig(out_fp)

    plt.show()


def plot_contingency_matrix(true, pred, out_fp=None):
    sns.set(font_scale=2)
    true_class_labels = np.unique(true, return_inverse=True)[0]
    pred_class_labels = np.unique(pred, return_inverse=True)[0]
    cm = metrics.cluster.contingency_matrix(true, pred)
    fig, ax = plt.subplots(figsize=(8,6))
    sns.heatmap(cm, annot=True, ax=ax, fmt='g', cmap="Greens")
    ax.set_xlabel('Predicted labels')
    ax.set_ylabel('True labels') 
    ax.xaxis.set_ticklabels(pred_class_labels)
    ax.yaxis.set_ticklabels(true_class_labels)

    if out_fp is not None:
        fig.savefig(out_fp)
        
    plt.show()


lr = lambda x, y: np.log2(x / y)

def calculate_cluster_lrs(c1_counts, c2_counts, n_words_1, n_words_2):
    lrs = dict()
    for word, count in c1_counts.items():
        lrs[word] = lr((count + 0.5) / n_words_1, (c2_counts[word] + 0.5) / n_words_2)
    return lrs