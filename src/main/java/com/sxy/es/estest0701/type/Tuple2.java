package com.sxy.es.estest0701.type;

//两个元素的元组，用于在一个方法里面返回两种类型的值
public class Tuple2<T1,T2> {
    private T1 _1;
    private T2 _2;

    public Tuple2(T1 _1, T2 _2){
        this._1 = _1;
        this._2 = _2;
    }

    public T1 _1(){return _1;}

    public T2 _2(){return _2;}
}
