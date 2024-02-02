import pandas as pd 
import sqlite3 as sql 
import tabulate as tbl 
import matplotlib.pyplot as plt
import seaborn as sns
import mplcursors
import sys


#  Connect to SQLite database
conn = sql.connect("7773mera25.db")

def fech_all(conn, table):
    if table:
        # Returns all the appendancies from a table in a pd dataframe format
        q = f"SELECT * FROM {table};"
        df = pd.read_sql_query(q, conn)
        return df
    

def starter(data):
    data = data.replace('Not available', None)
    data = data.drop_duplicates()
    # print(f"\ndata describe__: \n{data.describe()}\n")
    # print(f"{data} head__:\n{data.head()}\n")
    return data

def Scatter_simple(data, name):
    # Set a Seaborn style for better aesthetics
    sns.set(style="whitegrid")

    # Create a figure and axis
    fig, ax = plt.subplots(figsize=(13, 6))

    # Create a scatter plot
    scatter = ax.scatter(range(len(data)), data, label='Data Points', color='blue')

    # Add labels and title
    ax.set_xlabel(f'Range of {name}')
    ax.set_ylabel(f'Data {name}')
    ax.set_title('Interactive Scatter Plot of Data')
    plt.show()

def Scatter(dataframe, column, figsize=(13, 6)):
    data = starter(dataframe[column])
    # Set the figure size
    plt.figure(figsize=figsize)

    # Create a scatter plot
    scatter = plt.scatter(range(len(data)), data, label='Data Points', color='blue')

    # Add labels and title
    plt.xlabel(f'Range of {data}')
    plt.ylabel(f'Data {data}')
    plt.title('Interactive Scatter Plot of Data')
    try:
        # Add interactivity using mplcursors
        cursor = mplcursors.cursor(scatter)

        def on_add(sel):
            index = sel.target.index
            annotation_text = f"Index: {index}, y: {data[index]}\n"
            annotation_text += f"All elements:\n{dataframe.iloc[index]}"
            sel.annotation.set_text(annotation_text)

        cursor.connect("add", on_add)
    except Exception as e:
        print(f"An error occurred: {e}")
    

    # Show the plot
    plt.grid()
    plt.show()

def IQR(data):
    # Calculate the IQR for the column
    Q1 = data.quantile(0.25)
    Q3 = data.quantile(0.75)
    IQR = Q3 - Q1

    # Define the lower and upper bounds for outliers
    lower_bound = Q1 - 1.5 * IQR
    upper_bound = Q3 + 1.5 * IQR

    # Remove outliers
    df_filtered = data[(data >= lower_bound) & (data <= upper_bound)]
    clear_data = data.drop(data[(data < lower_bound) | (data > upper_bound)].index, inplace=True)
    # print(df_filtered) 
    return df_filtered

def plot_pie(sizes, labels):
    # Plot
    plt.pie(sizes, labels=labels, autopct='%1.1f%%', shadow=True, startangle=140)
    plt.axis('equal')

    # Add legend to the side
    plt.legend(labels, loc="center left", bbox_to_anchor=(13/19, 12/100))

    plt.show()

def seperate_party_data(name_party):
    vids_df = fech_all(conn, "Vids")
    playlists_df = fech_all(conn, "Playlists")
    # print(playlists_df.columns)

    party_vids_df = vids_df[vids_df['author'] == name_party]
    # party_playlists_df = playlists_df[playlists_df["author"]== name_party] 
    return party_vids_df

def find_persentage_of_available_likes(name_party):
    party_vids_df = seperate_party_data(name_party)
    
    overal_number_of_likes = len(party_vids_df["likes"])
    likes = party_vids_df["likes"]

    not_avail = 0
    avail = 0
    for atribute in likes:
        if atribute == "Not available":
            not_avail+= 1
        else:
            avail+= 1
    if not_avail != 0:
        print(f"(available/not_available) = {avail/not_avail}")
    else:
        print(f"All the {overal_number_of_likes} like counters are available")

# def is_ordered(series)

# find_persentage_of_available_likes("To Potami")

Vids = fech_all(conn, "Vids") 
Vids = starter(Vids)
# print(Vids.columns)
# sys.exit()
Playlists = fech_all(conn, "Playlists") 
Channels = fech_all(conn, "Channels") 

vid_views = Vids["views"]
# Scatter(vid_views)
# video_views_desc = vid_views.describe()

# Plot likes of videos
likes = Vids["likes"]
likes = starter(likes)
clear_likes = IQR(likes)
# Scatter(Vids, "likes")
# print(likes[:100])

# Plot lengths of videos
lengths = Vids["length"]
lengths = IQR(lengths)
lenghts = starter(lengths)
# Scatter(clear_lengths)
# Scatter(Vids, "length")

Relations = fech_all(conn, "Relations")
# print(Relations.head())
related_channel_id = Relations["related_channel_id"]
unique_ids = related_channel_id.unique()
# print(unique_ids)
# sys.exit() 
# print(related_channel_id.head())

# Plot pie chart for the volume of the videos
# Count occurrences of each unique value
sizes = related_channel_id.value_counts()
# print(len(sizes))
# sys.exit()
labels = [i for i in range(len(sizes))]

labels = [    
"anexartitoiellines",
"ΚΟΜΜΟΥΝΙΣΤΙΚΟ ΚΟΜΜΑ ΕΛΛΑΔΑΣ",
"PASOKwebTV",
"Αντιδιαπλοκή ΕΝΩΣΗ ΚΕΝΤΡΩΩΝ",
"Laiki Enotita",              
"To Potami"]
# print(len(sizes))
# sys.exit()

# plot_pie(sizes,labels)







# List tables in the database
# tables_q = "SELECT name FROM sqlite_master WHERE type='table';"
# tables = conn.execute(tables_q).fetchall()

# Print the list of tables
# print("Tables in the database:")
# for table in tables:
#     print(table[0])

# # Query data into a DataFrame
# query = "SELECT * FROM Vids;" 
# vids_data = pd.read_sql_query(query, conn)
# print(tbl(vids_df.head(),headers="keys",tablefmt='pretty'))
conn.close()