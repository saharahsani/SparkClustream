package com.backhoff.clustream;

import scala.Int;

import java.util.ArrayList;
import java.util.List;

public class Setting {
    static String snapsPath = "src/test/resources/snaps";
    static int k = 5;
    static int h = 1;
    static int numPoints = 5000;
    static Integer unitTimeDigit1 = 10000;
    static Integer unitTimeDigit2 = 40000;
    static Integer unitTimeDigit3 = 160000;
    static Integer unitTimeDigit4 = 320000;
    static Integer unitTimeDigit5 = 840000; //578000;
    static Integer unitTimeDigit6 = 1080000;
    static Integer unitTimeDigit7 = 2200000;
    static Integer unitTimeDigit8 = 3380000;
    // set init path
    static String initPathFile = "src/test/resources/initClusters/kdd/";
    static Boolean initialize=true;
    // for sliding window
    static Integer windowTime = 70;
    static String centersOnlinePath = "src/test/resources/centersCov2_sw";
    static Integer centersStartNum = 1;
    static Boolean expirePhase=true;
}
