# Databricks notebook source
import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt
import plotly.figure_factory as ff
import numpy as np
import plotly.graph_objects as go
import matplotlib.colors as clr
import matplotlib 

def create_two_var_scatterplot(pandas_df: pd.DataFrame, 
                        x_col_name: str, 
                        y_col_name: str, 
                        z_col_name: str,
                        title: str = None,
                        colorscale: list[tuple] = None,
                        size: tuple = None
                        ) -> plt.figure:
    """
    Create a two variable scatterplot colored by a third column leveraging the Python Plotly Express library

    Args:
        pandas_df: The Pandas Dataframe to plot
        x_col_name: The name of the horizontal axis
        y_col_name: The name of the vertical axis
        z_col_name: The name of the variable to colorby
        title [optional]: The title of the plot 
        colorscale [optional]: The continous colorscale
        size [optional]: The size of the plot

    Returns:
        A Matplotlib Figure object that can be used to print

    Assumptions:
        Assumes the data passed is the subset you want to plot (i.e. pandas_df.iloc[0:100], etc.)
        If passing a custom colorscale it must take the form [(0, 'red'), (0.5, "green"), (1, "yellow")]
        where the first argument in each tuple is the position to transition and the second is the color as 
        named color or hex code
    """

    # set size in pixel if passed
    if size:
        width = size[0]
        height = size[1]
    else:
        width = 1000
        height = 800

    # default to Suncor colors if none passed
    if not colorscale:
        colorscale = ['#FFC429','#EF4135']

    fig = px.scatter(pandas_df, 
                     x=x_col_name,
                     y=y_col_name, 
                     color=z_col_name, 
                     color_continuous_scale=colorscale, 
                     title=title,
                     width=width,
                     height=height)
        
    return fig

def create_n_var_histogram(pandas_df: pd.DataFrame, 
                    col_names: list,
                    bin_size: float = None,
                    title: str = None,
                    colorscale: list = None,
                    size: tuple = None
                    ) -> plt.figure:
    """
    Create a overlapping distplot of multiple columns with optional colormap and size

    Args:
        pandas_df: The Pandas Dataframe to plot
        col_names: The name of Dataframe column to plot
        bin_size [optional]: The size of the histogram bins
        title [optional]: The title of the plot 
        colorscale [optional]: The continous colorscale
        size [optional]: The size of the plot

    Returns:
        A Matplotlib Figure object that can be used to print

    Assumptions:
        Assumes the data passed is the subset you want to plot (i.e. pandas_df.iloc[0:100], etc.)
        If passing a custom colorscale it must take the form ['red', 'green', 'blue'] as named color or hex code
    """

    if not colorscale:
        # suncor colormap
        cmap = clr.LinearSegmentedColormap.from_list('suncor_custom', ['#FFC429','#EF4135'], N=256)
        colorscale = [matplotlib.colors.to_hex(list(cmap(i))) for i in np.linspace(0, 1, len(col_names))]

    # remove any NaN in each column or plot will fail
    data = [pandas_df[name].dropna() for name in col_names]

    # default to size 1 if no bin_size passed
    if not bin_size:
        bin_size = 1

    # default to Suncor colors if none passed
    if not colorscale:
        colorscale = ['#FFC429', "#F58220", "#EF4135"]

    fig = ff.create_distplot(data,
                             col_names, 
                             colors=colorscale,
                             bin_size=bin_size,
                             show_rug=False)
    
    # set title if passed
    if title:    
        fig.update_layout(title_text=title)

    # set size in pixel if passed
    if size:
        fig.update_layout(width=size[0], height=size[1])
    
    return fig

def create_outlier_plot(pandas_df: pd.DataFrame, 
                 col_names: list,
                 title: str = None,
                 size: tuple = None
                 ) -> plt.figure:
    """
    Create a multi-varibale box plot for outlier detection

    Args:
        pandas_df: The Pandas Dataframe to plot
        col_names: The name of Dataframe column to plot
        title [optional]: The title of the plot 
        size [optional]: The size of the plot

    Returns:
        A Matplotlib Figure object that can be used to print

    Assumptions:
        Assumes the data passed is the subset you want to plot (i.e. pandas_df.iloc[0:100], etc.)
    """
    
    # create Dataframe of only columns wanted
    data = pandas_df[col_names]

    # set size in pixel if passed
    if size:
        width = size[0]
        height = size[1]
    else:
        width = 1000
        height = 800

    fig = px.box(data, 
                 points='outliers', 
                 title=title, 
                 width=width, 
                 height=height)
    
    return fig

def create_n_var_scatterplot(pandas_df: pd.DataFrame, 
                      col_names: list = None,
                      z_col_name: str = None,
                      title: str = None,
                      size: tuple = None
                      ) -> plt.figure:
    """
    Create a multi-varibale box plot for outlier detection

    Args:
        pandas_df: The Pandas Dataframe to plot
        col_names: The name of Dataframe column to plot
        z_col_name: The name of the column to colorby
        title [optional]: The title of the plot 
        size [optional]: The size of the plot

    Returns:
        A Matplotlib Figure object that can be used to print

    Assumptions:
        Assumes the data passed is the subset you want to plot (i.e. pandas_df.iloc[0:100], etc.) and is numeric data
    """
    # select only numeric columns and names
    if col_names:
        data = pandas_df[col_names].select_dtypes(include=np.number)
        col_names = data.columns
    else:
        data = pandas_df.select_dtypes(include=np.number)
        col_names = data.columns

    # set size in pixel if passed
    if size:
        width = size[0]
        height = size[1]
    else:
        width = 1000
        height = 800

    fig = px.scatter_matrix(data, 
                            dimensions=col_names,
                            color=z_col_name,
                            title=title, 
                            width=width, 
                            height=height)
    
    return fig

def create_cluster_stack_barplot(pandas_df: pd.DataFrame, 
                          x_axis_variable: str,
                          y_axis_variable: str,
                          group_by_variable: str,
                          stack_by_variable: str,
                          y_axis_name: str = None,
                          x_axis_name: str = None,
                          title: str = None,
                          size: tuple = None
                          ) -> plt.figure:
    """
    Create a stacked and grouped barchart

    Args:
        pandas_df: The Pandas Dataframe to plot
        x_axis_variable: The horizontal axis variable
        y_axis_variable: The vertical axis variable
        group_by_variable: The variable to groupby on horizontal axis
        stack_by_variable: The variable to stackby on vertical axis
        y_axis_name [optional]: The name of the y_axis
        x_axis_name [optional]: The name of the x_axis
        title [optional]: The title of the plot 
        size [optional]: The size of the plot

    Returns:
        A Matplotlib Figure object that can be used to print

    Assumptions:
        Assumes the data passed has been groupby and aggregated like below
        #             StationName  Month  Cnt             DayType
        # 0        CALGARY INTL A      1  312       AboveZeroDays
        # 1        CALGARY INTL A      2  231       AboveZeroDays
        # 2        CALGARY INTL A      3  327       AboveZeroDays
        # 3        CALGARY INTL A      4  353       AboveZeroDays
        # 4        CALGARY INTL A      5  591       AboveZeroDays
        # ..                  ...    ...  ...                 ...
        # 43  RED DEER REGIONAL A      5   18  BelowEqualZeroDays
        # 44  RED DEER REGIONAL A      9    5  BelowEqualZeroDays
    """
    
    # set size in pixel if passed
    if size:
        width = size[0]
        height = size[1]
    else:
        width = 1000
        height = 800

    fig = go.Figure()

    fig.update_layout(
        xaxis=dict(title_text=x_axis_name),
        yaxis=dict(title_text=y_axis_name),
        barmode="stack",
        width=width,
        height=height,
        title=title
    )

    colors = px.colors.sequential.Viridis

    for r, c in zip(pandas_df[stack_by_variable].unique(), colors):
        plot_df = pandas_df[pandas_df[stack_by_variable] == r]
        fig.add_trace(
            go.Bar(x=[plot_df[x_axis_variable], plot_df[group_by_variable]], y=plot_df[y_axis_variable], name=str(r), marker_color=c),
        )
    
    return fig
