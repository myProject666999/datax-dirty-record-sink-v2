package com.alibaba.datax.core.replay;

import java.io.File;

/**
 * 回放命令的解析结果。
 */
public class ReplayConfig {

    private File sinkDir;

    private File jobFile;

    private boolean force;

    private boolean allowManifestMissing;

    private File reDirtySinkDir;

    public File getSinkDir() {
        return sinkDir;
    }

    public void setSinkDir(File sinkDir) {
        this.sinkDir = sinkDir;
    }

    public File getJobFile() {
        return jobFile;
    }

    public void setJobFile(File jobFile) {
        this.jobFile = jobFile;
    }

    public boolean isForce() {
        return force;
    }

    public void setForce(boolean force) {
        this.force = force;
    }

    public boolean isAllowManifestMissing() {
        return allowManifestMissing;
    }

    public void setAllowManifestMissing(boolean allowManifestMissing) {
        this.allowManifestMissing = allowManifestMissing;
    }

    public File getReDirtySinkDir() {
        return reDirtySinkDir;
    }

    public void setReDirtySinkDir(File reDirtySinkDir) {
        this.reDirtySinkDir = reDirtySinkDir;
    }
}
