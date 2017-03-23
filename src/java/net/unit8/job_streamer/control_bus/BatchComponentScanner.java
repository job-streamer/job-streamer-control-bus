package net.unit8.job_streamer.control_bus;

import java.io.File;
import java.io.IOException;
import java.io.Serializable;
import java.lang.annotation.Annotation;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;

import javax.batch.api.Batchlet;
import javax.batch.api.chunk.ItemProcessor;
import javax.batch.api.chunk.ItemReader;
import javax.batch.api.chunk.ItemWriter;
import javax.batch.api.chunk.listener.ChunkListener;
import javax.batch.api.chunk.listener.ItemProcessListener;
import javax.batch.api.chunk.listener.ItemReadListener;
import javax.batch.api.chunk.listener.ItemWriteListener;
import javax.batch.api.chunk.listener.RetryProcessListener;
import javax.batch.api.chunk.listener.RetryReadListener;
import javax.batch.api.chunk.listener.RetryWriteListener;
import javax.batch.api.chunk.listener.SkipProcessListener;
import javax.batch.api.chunk.listener.SkipReadListener;
import javax.batch.api.chunk.listener.SkipWriteListener;
import javax.batch.api.listener.JobListener;
import javax.batch.api.listener.StepListener;
import javax.inject.Named;

import com.google.common.base.Strings;

/**
 * @author kawasima
 */
public class BatchComponentScanner {
    private BatchComponentContainer container = new BatchComponentContainer();
    private final static Set<Class<?>> LISTENER_INTERFACES = 
            new HashSet(Arrays.asList(JobListener.class,StepListener.class,ChunkListener.class,
                    ItemProcessListener.class,ItemReadListener.class,ItemWriteListener.class,
                    RetryProcessListener.class,RetryReadListener.class,RetryWriteListener.class,
                    SkipProcessListener.class,SkipReadListener.class,SkipWriteListener.class));

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
        System.err.println("decideRefName: clazz: " + clazz);
        System.err.println("decideRefName: annotation: " + clazz.getAnnotations());
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
            System.err.println("className: " + className);
            if (className.startsWith("java.")
                    || className.startsWith("javax.")
                    || className.startsWith("com.sun.")
                    || className.startsWith("sun.")
                    || className.startsWith("clojure.")) {
                System.err.println("This is ignored with wrong package name");
                return;
            }
            try {
                Class<?> clazz = cl.loadClass(className);
                if (Batchlet.class.isAssignableFrom(clazz)) {
                  System.err.println("  This clazz is Batchlet");
                  container.batchlets.add(decideRefName(clazz));
                } else if (ItemReader.class.isAssignableFrom(clazz)) {
                  System.err.println("  This clazz is ItemReader");
                  container.itemReaders.add(decideRefName(clazz));
                } else if (ItemWriter.class.isAssignableFrom(clazz)) {
                  System.err.println("  This clazz is ItemWriter");
                  container.itemWriters.add(decideRefName(clazz));
                } else if (ItemProcessor.class.isAssignableFrom(clazz)) {
                  System.err.println("  This clazz is ItemProcessor");
                  container.itemProcessors.add(decideRefName(clazz));
                } else if (isListener(clazz)) {
                  System.err.println("  This clazz is Listener");
                  container.listeners.add(decideRefName(clazz));
                } else if (Throwable.class.isAssignableFrom(clazz)) {
                  System.err.println("  This clazz is Throwable");
                  container.throwables.add(decideRefName(clazz));
                } else {
                  System.err.println("  This clazz is ignored");
                }
            } catch (ClassNotFoundException | NoClassDefFoundError e) {
                System.err.println("  This clazz is ignored with Exception");
                e.printStackTrace();
                // ignore
            }
        }
    }
    
    private boolean isListener(Class<?> clazz){
        for(Class<?> listenerInterface:LISTENER_INTERFACES){
            if(listenerInterface.isAssignableFrom(clazz)){
                return true;
            }
        }
        return false;
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
        for (String listener : scanner.container.listeners) {
            System.out.println("listener:"+ listener);
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
        public Set<String> listeners = new HashSet<>();
        public Set<String> throwables = new HashSet<>();

        @Override
        public String toString() {
            return "Batchlet:" + batchlets + "\n" +
                    "ItemReader:" + itemReaders + "\n" +
                    "ItemWriter:" + itemWriters + "\n" +
                    "ItemProcessor:" + itemProcessors + "\n" +
                    "Listener:" + listeners + "\n" +
                    "Throwable:" + throwables + "\n";
        }
    }
}
