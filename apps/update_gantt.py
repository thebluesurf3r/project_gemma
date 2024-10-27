import streamlit as st
import psycopg2
from datetime import datetime

# Function to create the form
def create_team_member_form():
    st.title("Project Status Update")

    # Form to collect information
    with st.form("team_member_form"):
        member_name = st.text_input("Team Member Name")
        email = st.text_input("Email")
        task = st.text_input("Task")
        milestone = st.text_input("Milestone")
        progress = st.slider("Progress (%)", min_value=0, max_value=100, value=0, step=1)
        priority_options = ["High", "Medium", "Low"]
        priority = st.selectbox("Priority", options=priority_options)
        cost = st.number_input("Cost ($)", min_value=0, value=0)
        duration = st.number_input("Duration (days)", min_value=0, value=0)
        start_date = st.date_input("Start Date", value=datetime.today())
        end_date = st.date_input("End Date", value=datetime.today())

        submitted = st.form_submit_button("Submit")

        if submitted:
            # Input validation
            if not member_name or not email or not task or not milestone:
                st.error("Please fill in all required fields.")
            else:
                # Connect to the PostgreSQL database
                try:
                    connection = psycopg2.connect(
                    host=st.secrets["database"]["host"],
                    database=st.secrets["database"]["name"],
                    user=st.secrets["database"]["user"],
                    password=st.secrets["database"]["password"]
                    )
                    cursor = connection.cursor()
                    
                    # Insert data into the gantt_chart_data table
                    insert_query = """
                    INSERT INTO gantt_chart_data (member_name, email, task, milestone, progress, priority, cost, duration, start_date, end_date)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s);
                    """
                    cursor.execute(insert_query, (member_name, email, task, milestone, progress, priority, cost, duration, start_date, end_date))
                    connection.commit()
                    cursor.close()
                    connection.close()
                    
                    st.success("Data updated successfully!")
                except Exception as e:
                    st.error(f"Error occurred: {e}")

# Run the app
if __name__ == "__main__":
    create_team_member_form()