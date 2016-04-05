package net.unit8.job_streamer.control_bus;

import com.google.common.base.Strings;

import javax.batch.api.Batchlet;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.inject.Named;
import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
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
    private BatchComponentContainer container = new BatchComponentContainer();

    Set<String>  jarEntryNames(File zipFile) throws IOException {
        Set<String> entryNames = new HashSet<>();
        Enumeration<? extends ZipEntry> entriesEnum = new ZipFile(zipFile).entries();
        while(entriesEnum.hasMoreElements()) {
            ZipEntry entry = entriesEnum.nextElement();
            entryNames.add(entry.getName());
        }
        return entryNames;
    }

    String decideRefName(Class<?> clazz) {
        System.err.println(clazz);
        System.err.println(clazz.getAnnotations());
        Annotation[] annotations = clazz.getDeclaredAnnotations();
        for (Annotation annotation : annotations) {
            if (annotation instanceof Named && !Strings.isNullOrEmpty(((Named) annotation).value())) {
                return ((Named) annotation).value();
            }
        }
        return clazz.getName();
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
                    container.batchlets.add(decideRefName(clazz));
                } else if (ItemReader.class.isAssignableFrom(clazz)) {
                    container.itemReaders.add(decideRefName(clazz));
                } else if (ItemWriter.class.isAssignableFrom(clazz)) {
                    container.itemWriters.add(decideRefName(clazz));
                } else if (ItemProcessor.class.isAssignableFrom(clazz)) {
                    container.itemProcessors.add(decideRefName(clazz));
                } else if (Throwable.class.isAssignableFrom(clazz)) {
                    container.throwables.add(decideRefName(clazz));
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
        URLClassLoader cl = new URLClassLoader(urls.toArray(new URL[args.length]), BatchComponentContainer.class.getClassLoader());
        BatchComponentScanner scanner = new BatchComponentScanner();
        scanner.findBatchComponents(cl);
        for (String batchlet : scanner.container.batchlets) {
            System.out.println("batchlet:" + batchlet);
        }
        for (String itemReader : scanner.container.itemReaders) {
            System.out.println("item-reader:" + itemReader);
        }
        for (String itemWriter : scanner.container.itemWriters) {
            System.out.println("item-writer:" + itemWriter);
        }
        for (String itemProcessor : scanner.container.itemProcessors) {
            System.out.println("item-processor:"+ itemProcessor);
        }
        for (String throwable : scanner.container.throwables) {
            System.out.println("throwable:" + throwable);
        }
    }

    static class BatchComponentContainer implements Serializable {
        public Set<String> batchlets = new HashSet<>();
        public Set<String> itemReaders = new HashSet<>();
        public Set<String> itemWriters = new HashSet<>();
        public Set<String> itemProcessors = new HashSet<>();
        public Set<String> throwables = new HashSet<>();

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
