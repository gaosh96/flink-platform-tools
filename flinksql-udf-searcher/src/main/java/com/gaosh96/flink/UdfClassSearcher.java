package com.gaosh96.flink;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

import static com.gaosh96.flink.ConfigConstants.AGGREGATE_FUNCTION_CLASS;
import static com.gaosh96.flink.ConfigConstants.CLASS_SUFFIX;
import static com.gaosh96.flink.ConfigConstants.SCALAR_FUNCTION_CLASS;
import static com.gaosh96.flink.ConfigConstants.TABLE_AGGREGATE_FUNCTION;
import static com.gaosh96.flink.ConfigConstants.TABLE_FUNCTION_CLASS;

/**
 * @author gaosh
 * @version 1.0
 * @date 2023/6/20
 */
public class UdfClassSearcher {

    static final List<String> udfInterfaceList = new ArrayList<>();

    static {
        udfInterfaceList.add(SCALAR_FUNCTION_CLASS);
        udfInterfaceList.add(TABLE_FUNCTION_CLASS);
        udfInterfaceList.add(AGGREGATE_FUNCTION_CLASS);
        udfInterfaceList.add(TABLE_AGGREGATE_FUNCTION);
    }

    public static List<String> getUdfClassList(String jarFilePath) throws IOException {

        List<String> udfClassList = new ArrayList<>();

        // maybe cause IOException
        JarFile jarFile = new JarFile(new File(jarFilePath));
        UDFClassLoader udfClassLoader = new UDFClassLoader(jarFilePath);

        // start traverse
        Enumeration<JarEntry> entries = jarFile.entries();
        while (entries.hasMoreElements()) {
            JarEntry entry = entries.nextElement();
            if (!entry.isDirectory() && entry.getName().endsWith(CLASS_SUFFIX)) {
                String className = entry.getName().replace("/", ".").substring(0, entry.getName().length() - 6);
                Class<?> clazz;
                try {
                    clazz = udfClassLoader.loadClass(className);
                } catch (ClassNotFoundException e) {
                    continue;
                }

                if (clazz.isInterface()) {
                    continue;
                }

                Class<?>[] interfaces = clazz.getInterfaces();
                for (Class<?> iface : interfaces) {
                    if (udfInterfaceList.contains(iface.getName())) {
                        udfClassList.add(className);
                    }
                }
            }
        }

        jarFile.close();

        return udfClassList;
    }

}
