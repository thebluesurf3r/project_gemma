import plotly.io as pio

# Define custom layout parameters
def get_custom_layout():
    return {
        "height": 800,  # Use ':' instead of '='
        "width": 1400,  # Use ':' instead of '='
        "plot_bgcolor": "black",  # Set the background color to black
        "paper_bgcolor": "black",  # Set the paper background color to black
        "font": {"family": "Arial", "size": 14, "color": "#FFFFFF"},  # Font settings
        "title": {"x": 0.5},  # Center-align title
        "margin": {"t": 60, "b": 40, "l": 40, "r": 40},  # Tight layout margins
        "xaxis": {
            "title_font": {"size": 12},
            "tickfont": {"size": 12},
            "showline": True,  # Show the horizontal line
            "zeroline": False,  # Hide the vertical line
            "gridcolor": "black"  # Color of the grid lines
        },
        "yaxis": {
            "title_font": {"size": 12},
            "tickfont": {"size": 12},
            "showline": True,  # Show the horizontal line
            "zeroline": False,  # Hide the vertical line
            "gridcolor": "gray"  # Color of the grid lines
        }
    }

# Set custom template based on 'plotly_dark' with updated layout
def set_custom_template():
    custom_layout = {
        "layout": get_custom_layout()
    }

    # Update the Plotly template
    pio.templates["custom_dark"] = pio.templates["plotly_dark"].update(custom_layout)
    pio.templates.default = "custom_dark"  # Use custom template as default
    pio.renderers.default = 'notebook'  # Render inline in notebook

# Call the function to set the custom template
#set_custom_template()  # Ensure this function is called to apply the template