import lombok.Data;

import java.io.Serializable;

@Data
public class MovieInfo implements Serializable {
    int serial;
    String name;
    String releaseDate;
    String videoReleaseDate;
    String imdbLink;
    int unknown;
    int action;
    int adventure;
    int animation;
    int children;
    int comedy;
    int crime;
    int documentary;
    int drama;
    int fantasy;
    int noir;
    int horror;
    int musical;
    int mystery;
    int romance;
    int scifi;
    int thriller;
    int war;
    int western;


}