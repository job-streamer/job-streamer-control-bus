package net.unit8.job_streamer.control_bus.watchdog;

import java.io.IOException;
import java.nio.file.*;
import java.util.List;

/**
 * @author kawasima
 */
public class FileCreateWatchdog {
    public void watch() throws IOException {
        Path dir = Paths.get("");
        WatchService watchService = createWatchService(dir);
        dir.register(watchService, StandardWatchEventKinds.ENTRY_CREATE);
        while(true) {
            try {
                WatchKey key = watchService.take();
                Path name;
                key.reset();
                List<WatchEvent<?>> events = key.pollEvents();
                for (WatchEvent<?> event : events) {
                    WatchEvent.Kind<?> kind =event.kind();
                    if (kind == StandardWatchEventKinds.OVERFLOW) continue;
                    name = (Path) event.context();
                    Path child = dir.resolve(name);

                    if (kind == StandardWatchEventKinds.ENTRY_CREATE) {
                        key.reset();
                    }

                }
            } catch (InterruptedException ignore) {

            }
        }
    }

    private WatchService createWatchService(Path path) throws IOException {
        return path.getFileSystem().newWatchService();
    }
}
