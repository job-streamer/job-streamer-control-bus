package net.unit8.job_streamer.control_bus;

import javax.batch.api.Batchlet;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

/**
 * @author kawasima
 */
public class BatchComponentScanner {
    private BatchComponentContaienr container = new BatchComponentContaienr();

    Set<String>  jarEntryNames(File zipFile) throws IOException {
        Set<String> entryNames = new HashSet<>();
        Enumeration<? extends ZipEntry> entriesEnum = new ZipFile(zipFile).entries();
        while(entriesEnum.hasMoreElements()) {
            ZipEntry entry = entriesEnum.nextElement();
            entryNames.add(entry.getName());
        }
        return entryNames;
    }

    void pickOverBatchComponent(String resourcePath, ClassLoader cl) {
        if (resourcePath.endsWith(".class")) {
            String className = resourcePath
                    .substring(0, resourcePath.lastIndexOf(".class"))
                    .replace('/', '.').replace('\\', '.');
            if (className.startsWith("java.")
                    || className.startsWith("javax.")
                    || className.startsWith("com.sun.")
                    || className.startsWith("sun.")
                    || className.startsWith("clojure.")) {
                return;
            }
            try {
                Class<?> clazz = cl.loadClass(className);
                if (Batchlet.class.isAssignableFrom(clazz)) {
                    System.out.println("batchlet:" + className);
                    container.batchlets.add((Class<? extends Batchlet>) clazz);
                } else if (ItemReader.class.isAssignableFrom(clazz)) {
                    container.itemReaders.add((Class<? extends ItemReader>) clazz);
                } else if (ItemWriter.class.isAssignableFrom(clazz)) {
                    container.itemWriters.add((Class<? extends ItemWriter>) clazz);
                } else if (ItemProcessor.class.isAssignableFrom(clazz)) {
                    container.itemProcessors.add((Class<? extends ItemProcessor>) clazz);
                } else if (Throwable.class.isAssignableFrom(clazz)) {
                    container.throwables.add((Class<? extends Throwable>) clazz);
                }
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                // ignore
            }
        }
    }

    void findClassInDir(final File dir, final ClassLoader loader) {
        try {
            Files.walkFileTree(dir.toPath(), new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) {
                    if (Files.isRegularFile(file)) {
                        pickOverBatchComponent(dir.toPath().relativize(file).toString(), loader);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            // ignore
        }
    }

    void findClassInJar(File jar, ClassLoader loader) {
        try {
            for (String name : jarEntryNames(jar)) {
                pickOverBatchComponent(name, loader);
            }
        } catch (IOException e) {
            // ignore
        }
    }

    void findBatchComponents(URLClassLoader loader) {
        PathMatcher matcher = FileSystems.getDefault().getPathMatcher("glob:*.{zip,jar}");

        for (URL url : loader.getURLs()) {
            if (url.getProtocol().equals("file")) {
                try {
                    File file = new File(url.toURI());
                    if (file.exists()) {
                        if (file.isDirectory()) {
                            findClassInDir(file, loader);
                        } else if (file.isFile() && matcher.matches(file.toPath().getFileName())) {
                            findClassInJar(file, loader);
                        }
                    }
                } catch (URISyntaxException e) {
                    // ignore
                }
            }

        }
    }

    public static void main(String... args) {
        List<URL> urls = new ArrayList<>(args.length);
        for (String arg : args) {
            try {
                urls.add(URI.create(arg).toURL());
            } catch(MalformedURLException e) {
                // ignore
            }
        }
        URLClassLoader cl = new URLClassLoader(urls.toArray(new URL[args.length]), BatchComponentContaienr.class.getClassLoader());
        BatchComponentScanner scanner = new BatchComponentScanner();
        scanner.findBatchComponents(cl);
        for (Class<? extends Batchlet> batchlet : scanner.container.batchlets) {
            System.out.println("batchlet:" + batchlet.getName());
        }
        for (Class<? extends ItemReader> itemReader : scanner.container.itemReaders) {
            System.out.println("item-reader:" + itemReader.getName());
        }
        for (Class<? extends ItemWriter> itemWriter : scanner.container.itemWriters) {
            System.out.println("item-writer:" + itemWriter.getName());
        }
        for (Class<? extends ItemProcessor> itemProcessor : scanner.container.itemProcessors) {
            System.out.println("item-processor:"+ itemProcessor.getName());
        }
        for (Class<? extends Throwable> throwable : scanner.container.throwables) {
            System.out.println("throwable:" + throwable.getName());
        }

    }

    static class BatchComponentContaienr implements Serializable {
        public Set<Class<? extends Batchlet>> batchlets = new HashSet<>();
        public Set<Class<? extends ItemReader>> itemReaders = new HashSet<>();
        public Set<Class<? extends ItemWriter>> itemWriters = new HashSet<>();
        public Set<Class<? extends ItemProcessor>> itemProcessors = new HashSet<>();
        public Set<Class<? extends Throwable>> throwables = new HashSet<>();

        @Override
        public String toString() {
            return "Batchlet:" + batchlets + "\n" +
                    "ItemReader:" + itemReaders + "\n" +
                    "ItemWriter:" + itemWriters + "\n" +
                    "ItemProcessor:" + itemProcessors + "\n" +
                    "Throwable:" + throwables + "\n";

        }
    }
}
