package com.a2.assignment.music;

import java.util.List;

public class SongCollection {
    private List<Song> songs;

    public SongCollection() {
    }

    public List<Song> getSongs() {
        return songs;
    }

    public void setSongs(List<Song> songs) {
        this.songs = songs;
    }
}