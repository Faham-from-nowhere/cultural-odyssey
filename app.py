import streamlit as st
import snowflake.connector
import pandas as pd
import plotly.express as px
import plotly.figure_factory as ff
from datetime import datetime
from fuzzywuzzy import process

# --- Page Config ---
st.set_page_config(page_title="Cultural Tourism for India", layout="wide")

# --- Background Styling ---
st.markdown(
    f"""
    <style>
    .stApp {{
        background-image: url("https://images.unsplash.com/photo-1477587458883-47145ed94245?q=80&w=1470&auto=format&fit=crop&ixlib=rb-4.1.0&ixid=M3wxMjA3fDB8MHxwaG90by1wYWdlfHx8fGVufDB8fHx8fA%3D%3D");
        background-size: cover;
        background-position: center;
        background-repeat: no-repeat;
        background-attachment: fixed;
    }}
    .block-container {{
        background-color: rgba(255, 255, 255, 0.85);
        padding: 2rem;
        border-radius: 1rem;
    }}
    </style>
    """,
    unsafe_allow_html=True
)

# --- App Header ---
st.title("Cultural Tourism for India")

# --- Connect to Snowflake ---
@st.cache_resource
def init_connection():
    return snowflake.connector.connect(
        user=st.secrets["snowflake"]["user"],
        password=st.secrets["snowflake"]["password"],
        account=st.secrets["snowflake"]["account"],
        warehouse=st.secrets["snowflake"]["warehouse"],
        database="HACKATHON",
        schema="PUBLIC"
    )

conn = init_connection()

# --- Load Data ---
def load_data(table):
    query = f"SELECT * FROM {table}"
    return pd.read_sql(query, conn)

footfall_df = load_data("CLEANED_TOURISM_FOOTFALL")
hotspots_df = load_data("CLEANED_CULTURAL_HOTSPOTS")
official_df = load_data("TOURISM_STATS")
best_places_df = load_data("TOP_PLACES_TO_VISIT")

best_places_df.columns = [
    "null", "zone", "state", "city", "name", "type", "establishment_year",
    "time_needed_in_hrs", "google_review_rating", "entrance_fee_in_inr",
    "airport_within_50km_radius", "weekly_off", "significance",
    "dslr_allowed", "number_of_google_reviews_in_lakhs", "best_time_to_visit"
]
best_places_df = best_places_df.drop(columns=['null'])

# --- Top Navigation ---
section = st.selectbox(
    "Navigate to",
    [
        "Cultural Map",
        "Tourism Stats",
        "Sustainability Travel Score",
        "QNA Query",
        "Best places to visit",
        "Find Hotels Nearby",
        "Unique Story",
        "Contact Us"
    ],
    index=0
)

# --- Cultural Map ---
if section == "Cultural Map":
    st.header("\U0001F30D Interactive Cultural Map")
    valid_hotspots_df = hotspots_df.dropna(subset=["LATITUDE", "LONGITUDE"])

    tab1, tab2 = st.tabs(["\U0001F5FA Scatter Map", "\U0001F525 Heatmap"])

    with tab1:
        fig = px.scatter_geo(
            valid_hotspots_df,
            lat='LATITUDE',
            lon='LONGITUDE',
            text='NAME',
            color='TYPE',
            hover_name='NAME',
            projection='natural earth'
        )
        fig.update_layout(title="Cultural Hotspots Scatter Plot")
        st.plotly_chart(fig, use_container_width=True)

    with tab2:
        heat_df = valid_hotspots_df[["LATITUDE", "LONGITUDE"]].copy()
        fig = ff.create_2d_density(
            heat_df['LONGITUDE'], heat_df['LATITUDE'],
            colorscale='Hot',
            point_size=3
        )
        fig.update_layout(title="Cultural Hotspot Heatmap (Density)")
        st.plotly_chart(fig, use_container_width=True)

# --- Tourism Stats ---
elif section == "Tourism Stats":
    st.header("\U0001F4CA Tourism Stats Across India")

    official_df.columns = [col.strip().lower().replace(" ", "_") for col in official_df.columns]
    official_df.rename(columns={'states/uts_*': 'state', 's._no.': 'sno'}, inplace=True)

    long_df = official_df.melt(
        id_vars=['state'],
        value_vars=[col for col in official_df.columns if 'domestic' in col or 'foreign' in col],
        var_name='type_year',
        value_name='visitors'
    )

    long_df['type'] = long_df['type_year'].apply(lambda x: 'domestic' if 'domestic' in x else 'foreign')
    long_df['year'] = long_df['type_year'].str.extract(r'(\d{4})')
    long_df.drop(columns='type_year', inplace=True)
    long_df.dropna(subset=['visitors'], inplace=True)

    fig = px.bar(
        long_df,
        x='state',
        y='visitors',
        color='type',
        facet_col='year',
        barmode='group',
        title="Domestic vs Foreign Tourist Visits by Year"
    )
    st.plotly_chart(fig, use_container_width=True)
    st.dataframe(long_df, use_container_width=True)

# --- Sustainability Travel Score ---
elif section == "Sustainability Travel Score":
    st.header("\U0001F3C6 Sustainability Travel Score")

    vehicle = st.selectbox("What type of vehicle will you use?", ["Electric Vehicle", "Public Transport", "Bike", "Car", "Flight"])
    stay = st.selectbox("Where will you stay?", ["Eco Lodge", "Budget Hotel", "Luxury Hotel"])
    food = st.selectbox("What type of food do you prefer?", ["Local Vegetarian", "Local Non-Vegetarian", "Fast Food Chains"])
    plastic = st.selectbox("Will you carry reusable items (bottles/bags)?", ["Yes", "No"])
    group = st.selectbox("Are you traveling solo or in a group?", ["Solo", "Group"])

    score = 0

    if vehicle == "Electric Vehicle": score += 30
    elif vehicle == "Public Transport": score += 25
    elif vehicle == "Bike": score += 20
    elif vehicle == "Car": score += 10
    elif vehicle == "Flight": score += 5

    if stay == "Eco Lodge": score += 25
    elif stay == "Budget Hotel": score += 15
    elif stay == "Luxury Hotel": score += 5

    if food == "Local Vegetarian": score += 15
    elif food == "Local Non-Vegetarian": score += 10
    elif food == "Fast Food Chains": score += 5

    if plastic == "Yes": score += 10
    if group == "Group": score += 10

    st.metric("\U0001F331 Sustainable Travel Score", score)

    if score >= 80:
        st.success("\U0001F3C5 Badge Unlocked: Eco Traveler")
    elif score >= 50:
        st.info("\U0001F396 Badge Unlocked: Culture Seeker")
    else:
        st.warning("\U0001F6B6 Start exploring to earn badges!")

    st.progress(score / 100)

# --- QNA ---
elif section == "QNA Query":
    st.header("\u2753 Ask about Tourism Places")
    user_question = st.text_input("Enter your question:")

    if user_question:
        cursor = conn.cursor()
        try:
            cursor.execute("SELECT c1, c2 FROM CLEANED_TRAIN")
            qna_pairs = cursor.fetchall()

            questions = [q[0] for q in qna_pairs]
            best_match, score = process.extractOne(user_question, questions)

            if score > 60:
                for q, a in qna_pairs:
                    if q == best_match:
                        st.markdown(f"**Matched Question:** {best_match} (Score: {score})")
                        st.markdown(f"**Answer:** {a}")
                        break
            else:
                st.warning("No matching answer found. Try rephrasing your question.")

        except Exception as e:
            st.error(f"Error querying database: {e}")
        finally:
            cursor.close()

# --- Best Places ---
elif section == "Best places to visit":
    st.header("\U0001F3DE Best Places to Visit in India")
    zones = best_places_df['zone'].dropna().unique()
    selected_zone = st.selectbox("\U0001F310 Select Zone:", sorted(zones))

    states = best_places_df.loc[best_places_df['zone'] == selected_zone, 'state'].dropna().unique()
    selected_state = st.selectbox("\U0001F3DB Select State:", sorted(states))

    cities = best_places_df.loc[
        (best_places_df['zone'] == selected_zone) & (best_places_df['state'] == selected_state),
        'city'
    ].dropna().unique()
    selected_city = st.selectbox("\U0001F3D9 Select City:", sorted(cities))

    places = best_places_df.loc[
        (best_places_df['zone'] == selected_zone) &
        (best_places_df['state'] == selected_state) &
        (best_places_df['city'] == selected_city),
        'name'
    ].dropna().unique()
    selected_place = st.selectbox("\U0001F4CD Select Place:", sorted(places))

    place_info = best_places_df.loc[
        (best_places_df['zone'] == selected_zone) &
        (best_places_df['state'] == selected_state) &
        (best_places_df['city'] == selected_city) &
        (best_places_df['name'] == selected_place)
    ]

    if not place_info.empty:
        place_info = place_info.iloc[0]
        st.markdown(f"### Details for **{selected_place}**")

        tab1, tab2, tab3 = st.tabs(["\U0001F4CB Overview", "⭐ Ratings & Fees", "\U0001F4C5 Visiting Info"])

        with tab1:
            st.write(f"**Zone:** {place_info['zone']}")
            st.write(f"**State:** {place_info['state']}")
            st.write(f"**City:** {place_info['city']}")
            st.write(f"**Type:** {place_info['type']}")
            st.write(f"**Significance:** {place_info['significance']}")
            st.write(f"**Establishment Year:** {place_info['establishment_year']}")

        with tab2:
            rating = place_info['google_review_rating']
            try:
                stars = "⭐" * int(round(float(rating)))
            except:
                stars = "N/A"
            st.write(f"**Google Review Rating:** {rating} {stars}")
            st.write(f"**Reviews (Lakhs):** {place_info['number_of_google_reviews_in_lakhs']}")
            st.write(f"**Entrance Fee (₹):** {place_info['entrance_fee_in_inr']}")

        with tab3:
            st.write(f"**Time to Visit (hrs):** {place_info['time_needed_in_hrs']}")
            st.write(f"**Best Time to Visit:** {place_info['best_time_to_visit']}")
            st.write(f"**Weekly Off:** {place_info['weekly_off']}")
            st.write(f"**DSLR Allowed:** {place_info['dslr_allowed']}")
            st.write(f"**Nearby Airport:** {place_info['airport_within_50km_radius']}")
# --- Find Hotels Nearby ---
elif section == "Find Hotels Nearby":
    st.header("\U0001F3E8 Find Hotels Near Cultural Hotspots")

    if 'hotspots_df' not in locals():
        hotspots_df = load_data("CLEANED_CULTURAL_HOTSPOTS")

    valid_hotspots = hotspots_df.dropna(subset=['LATITUDE', 'LONGITUDE'])
    selected_place = st.selectbox("Choose a Cultural Hotspot:", valid_hotspots['NAME'].unique())

    hotspot_info = valid_hotspots[valid_hotspots['NAME'] == selected_place].iloc[0]
    lat = hotspot_info['LATITUDE']
    lon = hotspot_info['LONGITUDE']

    st.map(pd.DataFrame([[lat, lon]], columns=["lat", "lon"]))

    st.markdown("### Hotels near this hotspot:")

    import requests

    GEOAPIFY_API_KEY = st.secrets["geoapify"]["api_key"]  # Store your key in .streamlit/secrets.toml

    params = {
        "categories": "accommodation.hotel",
        "filter": f"circle:{lon},{lat},3000",  # 3 km radius
        "limit": 10,
        "apiKey": GEOAPIFY_API_KEY
    }

    response = requests.get("https://api.geoapify.com/v2/places", params=params)

    if response.status_code == 200:
        places = response.json().get("features", [])
        if places:
            search_term = st.text_input("Search for a keyword in hotel names or addresses").lower()

            for place in places:
                props = place["properties"]
                name = props.get("name", "Unnamed Hotel")
                address = props.get("formatted", "No address")
                website = props.get("website", None)

                if search_term in name.lower() or search_term in address.lower():
                    st.subheader(name)
                    st.write(f"📍 {address}")
                    if website:
                        st.markdown(f"[Visit Website]({website})")
                    st.markdown("---")
        else:
            st.info("No hotels found nearby.")
    else:
        st.error("Failed to fetch data from Geoapify.")
# --- Unique Story Section ---
elif section == "Unique Story":
    # Change background for this section
    st.markdown(
        f"""
        <style>
        .stApp {{
            background-image: url("https://img1.wsimg.com/isteam/ip/dd7bf207-c00c-4b95-a0a3-ff510508d271/houseboat.jpeg/:/cr=t:0%25,l:0%25,w:100%25,h:100%25/rs=w:1280");
            background-size: cover;
            background-position: center;
            background-repeat: no-repeat;
            background-attachment: fixed;
        }}
        </style>
        """,
        unsafe_allow_html=True
    )
    
    st.header("✨ Unique Cultural Stories of India")
    
    # Load story data
    @st.cache_data
    def load_story_data():
        query = "SELECT * FROM CLEANED_TRAIN_STORYGUIDE"
        df = pd.read_sql(query, conn)
        # Rename columns for better readability
        df.columns = ['festival_name', 'description', 'region', 'category', 'subcategory', 'source']
        return df
    
    story_df = load_story_data()
    
    # Create dropdown with festival names
    selected_festival = st.selectbox(
        "Select a Festival to Explore its Story:",
        sorted(story_df['festival_name'].unique()),
        index=0
    )
    
    # Display the selected festival's story
    if selected_festival:
        festival_info = story_df[story_df['festival_name'] == selected_festival].iloc[0]
        
        st.markdown(f"### {festival_info['festival_name']}")
        st.markdown(f"**Region:** {festival_info['region']}")
        st.markdown(f"**Category:** {festival_info['category']}")
        st.markdown(f"**Subcategory:** {festival_info['subcategory']}")
        
        st.markdown("---")
        st.subheader("The Story")
        st.markdown(festival_info['description'])
        
        st.markdown("---")
        st.caption(f"Source: {festival_info['source']}")
        
        # Optional: Add some decorative elements
        st.markdown("---")
        cols = st.columns(3)
        with cols[1]:
            st.markdown("🎉 **Celebrate the Diversity of India!** 🎉")
# --- Contact Us ---
elif section == "Contact Us":
    st.header("\U0001F4E9 Contact Us")
    st.markdown("""
    ### Website Credits
    - Pranjal Srivastava
    - Faham
    - Kamran

    📧 For any queries, contact: [pranjalsrivastava014@gmail.com](mailto:pranjalsrivastava014@gmail.com)
    """)
