import plotly.express as px
import ncei

data = ncei.grab_table('JAN')
fig =px.scatter_mapbox(data, lat='latitude', lon='longitude', 
                hover_name="name", hover_data=["name", "temperature"],
                color='temperature', zoom=3, height=600)

fig.update_layout(mapbox_style="open-street-map")
fig.update_layout(margin={"r":0,"t":0,"l":0,"b":0})

fig.write_html("stations_scatter.html", include_plotlyjs='cdn')


