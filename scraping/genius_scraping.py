from requests import get, put, post
from collections import Counter
import time
import networkx
import numpy as np
import sys
from pyvis.network import Network
import threading

export_thread = None

generateId = lambda _type, _x : "{}:{}".format(_type, _x)
typed_degrees = lambda g : [(n, Counter([data["role"] for a, b, data in G.edges(n, data=True)])) for n in g.nodes]
name_map = lambda g : {n : data["name"] for n, data in g.nodes(data=True)}

def noneToEmpty(x):
    return {} if x is None else x

def iterate_artists(_dict):
    for _role, _artists in _dict.items():
        for _artist in _artists:
            yield _role, *_artist

def addNodesEdgesFromSong(G, song):
    singers = [(a["id"], a) for a in [song["primary_artist"]]+song.get("featured_artists", [])]
    producers = [(a["id"], a) for a in song.get("producer_artists", [])]
    writers = [(a["id"], a) for a in song.get("writer_artists", [])]
    #others = {"cust:{}".format(perf["label"]) : [(a["id"], a) for a in perf["artists"]] for perf in song["custom_performances"]}
    others = {}
    artists = {**{"off:singer" : singers, "off:producer" : producers, "off:writer" : writers}, **others}

    #new nodes
    G.add_nodes_from([(generateId("artist", _id), {**_artist, "type" : "artist"}) for _artists in artists.values() for _id, _artist in _artists])
    G.add_nodes_from([(generateId("song", song["id"]), {**song, "type" : "song", "name" : song["title"]})])

    #new edges
    G.add_edges_from([(generateId("artist", a_i), generateId("song", song["id"]), {"role" : a_r, "song_id" : song["id"], "song" : song["title"], "album" : noneToEmpty(song.get("album", {})).get("name", "no_album")}) for a_r, a_i, a_a in iterate_artists(artists)])

def querySong(_id, verbose=False):
    song = get("https://api.genius.com/songs/{}".format(str(_id)), headers=headers, params={"per_page" : 50}).json()["response"]["song"]
    if verbose:
        print("{} n°{} ({}) - {}".format(song["title"], song["id"], noneToEmpty(song.get("album", {})).get("name", "no_album"), ", ".join([a.get("name") for a in [song["primary_artist"]]+song.get("featured_artists", [])])))
    return song

def queryArtistSongsIds(_id, verbose=False):
    next_page = 1
    while next_page:
        req = get("https://api.genius.com/artists/{}/songs".format(str(_id)), headers=headers, params={"page" : str(next_page), "per_page" : 50}).json()["response"]
        if verbose:
            print("---------- Artist n°{} - page {}".format(_id, next_page))
        for song in req["songs"]:
            yield song["id"]
        next_page = req["next_page"]

def exportGraphViz(_g, out_path_vis):
    # queries writers per song
    query_1 = {_attr.get("name") : [_g.nodes[_artist].get('name') for _song, _artist, _e_attr in _g.edges(_id, data=True) if _e_attr["role"] == 'off:writer'] for _id, _attr in _g.nodes(data=True) if _attr.get("type") == 'song'}
    query_2 = [((_a_1, _a_2), _title) for _title, _artists in query_1.items() for _a_1 in _artists for _a_2 in _artists if _a_1!=_a_2]
    edges = {e : [] for e, t in query_2}
    for e, title in query_2:
        edges[e].append(title)

    writersG = networkx.Graph()
    writersG.add_edges_from([(*_e, {"color" : "orange", "value" : len(_titles), "title" : "{} : {}".format(len(_titles), ", ".join(_titles))}) for _e, _titles in edges.items()])
    color_map = {n : "green" if data.get("explored", -1)>0 else "blue" for n, data in _g.nodes(data=True)}
    networkx.set_node_attributes(_g, color_map, "color")

    nt = Network(height='1080px', width='1920px', notebook=False)
    nt.from_nx(writersG)
    nt.force_atlas_2based()
    nt.show_buttons(filter_=['physics'])
    nt.save_graph('{}.html'.format(out_path_vis))
    print("async : exported {}".format(out_path_vis))

def handleArtist(_g, _id, out_path_g, out_path_vis, verbose=True):
    for song_id in queryArtistSongsIds(_id, verbose=True):
        try:
            if generateId("song", song_id) not in _g.nodes:
                addNodesEdgesFromSong(_g, querySong(song_id, verbose))
                _g.nodes[generateId("artist", _id)]["explored"] = time.time()
        except JSONDecodeError:
            pass
    networkx.write_gpickle(_g, out_path_g)

    global export_thread
    try:
        if export_thread.isAlive():
            return
    except Exception as e:
        pass
    export_thread = threading.Thread(target=exportGraphViz, name="Export Graph Visu", args=(_g, out_path_vis))
    export_thread.start()

if __name__ == "__main__":

    arguments = ["out_path_graph", "out_path_vis", "start_songs_path"]
    arg_diff = len(sys.argv[1:])-len(arguments)
    if arg_diff<0 and arg_diff>1:
        print("Expected parameters : {}".format(" ".join(["<{}>".format(a) for a in arguments])))
        exit(1)

    out_path_graph, out_path_vis = sys.argv[1:len(arguments)]
    start_songs_path = None
    if len(sys.argv[1:])==len(arguments):
        start_songs_path = sys.argv[len(arguments)]

    TOKEN = ""
    with open("genius_token") as f:
        TOKEN = f.readlines()[0].replace("\n", "")
    headers = {'Authorization': 'Bearer {}'.format(TOKEN)}

    out_path_gpickle = "{}.gpickle".format(out_path_graph)
    try:
        G = networkx.read_gpickle(out_path_gpickle)
        print("Loaded {}".format(out_path_gpickle))
    except OSError as e:
        print(e)
        G = networkx.MultiGraph()

    if start_songs_path:
        with open(start_songs_path) as f:
            contents = f.readlines()
        songs = contents[0].replace("\n", "").split(", ")
        # QUERYING SONGS TO CREATE FIRST GRAPH FROM AUTHORS
        starting_artists = set()
        for title in songs:
            res = get("https://api.genius.com/search", headers=headers, params={"q" : title})
            starting_song_id = res.json()["response"]["hits"][0]["result"]["id"]
            song = querySong(starting_song_id, True)
            starting_artists = set([s.get("id") for s in [song["primary_artist"]]+song.get("featured_artists", [])]).union(starting_artists)
        ", ".join([str(i) for i in starting_artists])

        for starting_artist_id in starting_artists:
            handleArtist(G, starting_artist_id, out_path_gpickle, out_path_vis, verbose=True)

    while True:
        probabilities = {i:p for i, p in [(_id, (time.time()-_attr.get("explored", -1))/time.time()*(G.degree[_id]**3)) for _id, _attr in G.nodes(data=True) if _attr.get("type") == "artist"]  if p>.05}
        next_artist = np.random.choice(list(probabilities.keys()), p=[p/sum(probabilities.values()) for p in probabilities.values()]).split(":")[1]
        handleArtist(G, next_artist, out_path_gpickle, out_path_vis, verbose=True)
