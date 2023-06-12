class TemplateGraphPlotly:
    def __init__(self,fig,family_font,tickangle,paper_bgcolor,plot_bg_color,color,color_plot,size,linewidth,linecolor):
        self.fig = fig
        self.family_font = family_font
        self.tickangle = tickangle
        self.paper_bgcolor = paper_bgcolor
        self.plot_bgcolor = plot_bg_color
        self.color_axis = color
        self.size = size
        self.linewidth = linewidth
        self.linecolor = linecolor
        self.color_plot= color_plot
        
 
    def get_template_axes(self):
        self.fig.update_xaxes(
            #showline=True,
            linewidth=self.linewidth,
            linecolor=self.linecolor,
            tickangle=self.tickangle,
            tickfont=dict(
            family=self.family_font,
            color=self.color_axis,
            size=self.size,
            )
        )
        self.fig.update_yaxes(
            #showline=True,
            linewidth=self.linewidth,
            linecolor=self.linecolor,
            tickangle=self.tickangle,
            tickfont=dict(
            family=self.family_font,
            color=self.color_axis,
            size=self.size,
            )
        )

    def get_template_layout(self):
        self.fig.update_layout(
        paper_bgcolor=self.paper_bgcolor,
        plot_bgcolor=self.plot_bgcolor,
        margin=dict(t=100,l=50,r=50,b=50),
        legend=dict(xanchor="left",x=0,yanchor="top",y=1,orientation="h"),
        font=dict(size=self.size,family=self.family_font,color=self.color_plot),hovermode="x unified")
      #  hovermode="x unified"
        