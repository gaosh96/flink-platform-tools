package com.gaosh96.flink;


import java.io.File;
import java.io.IOException;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.gaosh96.flink.ConfigConstants.CLASS_SUFFIX;

/**
 * @author gaosh
 * @version 1.0
 * @date 2023/3/31
 */
public class UDFClassLoader extends ClassLoader {

    private String jarFilePath;

    public UDFClassLoader(String jarFilePath) {
        this.jarFilePath = jarFilePath;
    }

    @Override
    protected Class<?> findClass(String name) throws ClassNotFoundException {
        try {
            JarFile jarFile = new JarFile(new File(jarFilePath));
            JarEntry entry = jarFile.getJarEntry(name.replace(".", "/") + CLASS_SUFFIX);

            if (entry != null) {
                byte[] classBytes = new byte[(int) entry.getSize()];
                jarFile.getInputStream(entry).read(classBytes);
                return defineClass(name, classBytes, 0, classBytes.length);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } catch (ClassFormatError | NoClassDefFoundError e) {
            // 如果尝试加载 final 类会提示 java.lang.ClassFormatError: Invalid method attribute name index 0 in class file xxx
            // 还有可能会导致 NoClassDefFoundError
            // ignore
        }

        return super.findClass(name);
    }
}
