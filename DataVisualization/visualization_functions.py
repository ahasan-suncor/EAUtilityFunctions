import plotly.express as px
import pandas as pd
import matplotlib.pyplot as plt

def two_var_scatterplot(pandas_df: pd.DataFrame, 
                        x_col_name: str, 
                        y_col_name: str, 
                        z_col_name: str,
                        title: str = None,
                        colorscale: list[tuple] = None,
                        size: tuple = None,
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
    # set size if passed
    if size:
        width = size[0]
        height = size[1]
    else:
        width = 1000
        height = 800

    # default to Suncor colors if none passed
    if not colorscale:
        colorscale = [(0, '#FFC429'), (0.5, "#F58220"), (1, "#EF4135")]

    fig = px.scatter(pandas_df, 
                     x=x_col_name,
                     y=y_col_name, 
                     color=z_col_name, 
                     color_continuous_scale=colorscale, 
                     title=title,
                     width=width,
                     height=height)
    
    return fig