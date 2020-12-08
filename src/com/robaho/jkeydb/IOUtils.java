package com.robaho.jkeydb;

import java.io.File;

class IOUtils {
    static void purgeDirectory(File dir) {
        if(!dir.isDirectory())
            return;
        for (File file: dir.listFiles()) {
            if (file.isDirectory())
                purgeDirectory(file);
            file.delete();
        }
    }
}
