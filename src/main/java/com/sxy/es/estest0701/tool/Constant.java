package com.sxy.es.estest0701.tool;

public interface Constant {
    String DTYPE = "dtype";
    String ADMIN_CODE = "district";
    String ADMIN_NAME = "district_text";
    String ADDRESS = "address";
    String POI_TYPE = "class_id";
    String POI_TYPE_NAME = "class_text";
    String ENTITY_TYPE = "clas_id";
    String ENTITY_TYPE_NAME = "clas_text";
    String LON = "lon";
    String LNG = "lng";
    String LAT = "lat";
    String DISTANCE = "distance";
    String GEOMETRY = "geometry";
    String PRIORITY = "priority";
    String ES_POINT = "the_point";
    String ES_SHAPE = "the_shape";
    String LENGTH = "length";
    String AREA = "area";

    interface Category {
        String FRAMEWORK = "sdmap";
        String THEMES = "themes";
        String ENTITY = "entity";
        String POI = "poi";
    }

    interface Separator {
        /**
         * greater than
         */
        String GT = ">";
        /**
         * less than
         */
        String LT = "<";
        String BLANK = " ";
        String COMMA = ",";
        String SEMICOLON = ";";
    }
}
