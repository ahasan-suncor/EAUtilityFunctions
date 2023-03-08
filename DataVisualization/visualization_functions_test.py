import pandas as pd
import unittest
import visualization_functions as my_plt

class test_plotting_functions(unittest.TestCase):

    def test_two_var_scatterplot(self):

        # input data
        df = pd.DataFrame({
            'TempC': [10, 12, -13],
            'RelHum': [90, 30, 55],
            'WindSpdkmh': [10, 9, 30]
            })
        
        # layout settings
        x_col_name='TempC'
        y_col_name='RelHum'
        z_col_name='WindSpdkmh'
        title='Weather Data Scatter Plot'
        colorscale=[(0, '#FFC429'), (0.5, "#F58220"), (1, "#EF4135")]
        size=(1000,800)

        # call function and create figure object to test
        fig = my_plt.two_var_scatterplot(pandas_df=df, 
                                x_col_name=x_col_name, 
                                y_col_name=y_col_name, 
                                z_col_name=z_col_name,
                                title=title,
                                colorscale=colorscale,
                                size=size)
        
        # extract data from object and construct dataframe to test
        x = fig.to_ordered_dict()['data'][0]['x']
        y = fig.to_ordered_dict()['data'][0]['y']
        z = fig.to_ordered_dict()['data'][0]['marker']['color'] 

        df_output = pd.DataFrame({ 
            'TempC': x,
            'RelHum': y,
            'WindSpdkmh': z
            })
        
        self.assertEqual(True, df_output.equals(df))
        
        # test layout of figure object
        self.assertEqual(fig.to_ordered_dict()['layout']['title']['text'], title)
        self.assertEqual(fig.to_ordered_dict()['layout']['xaxis']['title']['text'], x_col_name)
        self.assertEqual(fig.to_ordered_dict()['layout']['yaxis']['title']['text'], y_col_name)
        self.assertEqual(fig.to_ordered_dict()['layout']['coloraxis']['colorbar']['title']['text'], z_col_name)
        self.assertEqual((fig.to_ordered_dict()['layout']['width'], fig.to_ordered_dict()['layout']['height']), size)

    def test_n_var_histogram(self):

        # input data
        df = pd.DataFrame({
            'TempC': [10, 12, -13],
            'RelHum': [90, 30, 55],
            'WindSpdkmh': [10, 9, 30]
            })
        
        # layout settings
        col_names=['TempC', 'RelHum', 'WindSpdkmh']
        bin_size=5
        title='Weather Data Distplot'
        colorscale=['#FFC429', "#F58220", "#EF4135"]
        size=(800,500)
    
        # call function and create figure object to test
        fig = my_plt.n_var_histogram(pandas_df=df, 
                                     col_names=col_names, 
                                     bin_size=bin_size, 
                                     title=title, 
                                     colorscale=colorscale, 
                                     size=size)
        
        # extract data from object and construct dataframe to test
        x1 = fig.to_ordered_dict()['data'][0]['x']
        x2 = fig.to_ordered_dict()['data'][1]['x']
        x3 = fig.to_ordered_dict()['data'][2]['x']

        df_output = pd.DataFrame({
            'TempC': x1,
            'RelHum': x2,
            'WindSpdkmh': x3
            })
        
        self.assertEqual(True, df_output.equals(df))

        # test layout of figure object
        self.assertEqual(fig.to_ordered_dict()['layout']['title']['text'], title)
        self.assertEqual((fig.to_ordered_dict()['layout']['width'], fig.to_ordered_dict()['layout']['height']), size)
        self.assertEqual(fig.to_ordered_dict()['data'][0]['xbins']['size'], bin_size)

        col_names_output = [fig.to_ordered_dict()['data'][0]['name'], fig.to_ordered_dict()['data'][1]['name'], fig.to_ordered_dict()['data'][2]['name']]
        self.assertListEqual(col_names_output, col_names)

    def test_outlier_plot(self):

        # input data
        df = pd.DataFrame({
            'TempC': [10, 12, -13],
            'RelHum': [90, 30, 55],
            'WindSpdkmh': [10, 9, 30]
            })
        
        # layout settings
        col_names=['TempC', 'RelHum', 'WindSpdkmh']
        title='Weather Data Distplot'
        size=(800,500)

        # call function and create figure object to test
        fig = my_plt.outlier_plot(pandas_df=df, 
                                  col_names=col_names, 
                                  title=title, 
                                  size=size)
        
        # extract data from object and construct dataframe to test
        cols = fig.to_ordered_dict()['data'][0]['x']
        y = fig.to_ordered_dict()['data'][0]['y']

        df_out = pd.DataFrame({
            cols[0]: y[0:3],
            cols[3]: y[3:6],
            cols[-1]: y[6:9]
            })
        
        self.assertEqual(True, df_out.equals(df))

        # test layout of figure object
        self.assertEqual(fig.to_ordered_dict()['layout']['title']['text'], title)
        self.assertEqual((fig.to_ordered_dict()['layout']['width'], fig.to_ordered_dict()['layout']['height']), size)

    def test_n_var_scatterplot(self):

        # input data
        df = pd.DataFrame({
            'TempC': [10, 12, -13],
            'RelHum': [90, 30, 55],
            'WindSpdkmh': [10, 9, 30]
            })
        
        # layout settings
        col_names=['TempC', 'RelHum', 'WindSpdkmh']
        z_col_name = 'TempC'
        title='Weather Data Distplot'
        size=(800,500)

        # call function and create figure object to test
        fig = my_plt.n_var_scatterplot(pandas_df=df,
                        col_names=col_names,
                        z_col_name=z_col_name,
                        title=title,
                        size=size)
        
        # test layout of figure object
        self.assertEqual(fig.to_ordered_dict()['layout']['title']['text'], title)
        self.assertEqual((fig.to_ordered_dict()['layout']['width'], fig.to_ordered_dict()['layout']['height']), size)

        col_names_output = [fig.to_ordered_dict()['data'][0]['dimensions'][0]['label'], fig.to_ordered_dict()['data'][0]['dimensions'][1]['label'], fig.to_ordered_dict()['data'][0]['dimensions'][2]['label']]
        self.assertListEqual(col_names_output, col_names)

    def test_cluster_stack_barplot(self):
        
        # input data
        df = pd.DataFrame({
            'StationName': ['CALGARY INTL A', 'CALGARY INTL A', 'CALGARY INTL A', 'CALGARY INTL A'],
            'Month': [1, 2, 1, 2],
            'Cnt': [10, 9, 30, 40],
            'DayType': ['AboveZeroDays', 'AboveZeroDays', 'BelowZeroDays', 'BelowZeroDays']
            })
        
        # layout settings
        x_axis_variable='StationName'
        y_axis_variable='Cnt'
        group_by_variable='DayType'
        stack_by_variable='Month'
        y_axis_name='Count'
        x_axis_name='Station Name'
        title='Weather Data Grouped for Above and Below Zero Days'
        size=(1000, 800)
        
        fig = my_plt.cluster_stack_barplot(pandas_df=df, 
                                           x_axis_variable=x_axis_variable, 
                                           y_axis_variable=y_axis_variable, 
                                           group_by_variable=group_by_variable, 
                                           stack_by_variable=stack_by_variable,
                                           y_axis_name=y_axis_name, 
                                           x_axis_name=x_axis_name,
                                           title=title,
                                           size=size)

        # test layout of figure object
        self.assertEqual(fig.to_ordered_dict()['layout']['title']['text'], title)
        self.assertEqual((fig.to_ordered_dict()['layout']['width'], fig.to_ordered_dict()['layout']['height']), size)
        self.assertEqual(fig.to_ordered_dict()['layout']['xaxis']['title']['text'], x_axis_name)
        self.assertEqual(fig.to_ordered_dict()['layout']['yaxis']['title']['text'], y_axis_name)


unittest.main(argv=[''], verbosity=2, exit=False)