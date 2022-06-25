package com.diagiac.flink;

public abstract class Query {

    public abstract void initialize();
    public abstract void processing();
    public abstract void preProcessing();
    public abstract void postProcessing();
}
