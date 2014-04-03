package teamdatum;

import java.io.BufferedReader;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * NOTE: FileService is currently restricted to text files that can be read line-by-line as
 * defined by java.io.FileReader.
 *
 * TODO: Purge empty openJobs if no observers exist
 * TODO: Test empty file edge case
 */
public class FileServiceImpl implements FileService {

    public static final FileServiceImpl singleton = new FileServiceImpl();

    static final int BLOCK_SIZE = 10000;
    static final int MAX_CONCURRENT_THREADS = 8;

    private final ExecutorService _executorService;
    private final Map<String, FileJob> _openJobs;

    FileServiceImpl() {
        _executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_THREADS);
        _openJobs = new HashMap<String, FileJob>();
    }

    synchronized public FileHandle open(String path, FileObserver observer) {
        FileHandle answer;
        FileJob job = _openJobs.get(path);
        if (job == null) {
            job = new FileJob(path);
            _openJobs.put(path, job);
        }
        answer = job.openWithObserver(observer);
        return answer;
    }

    synchronized public void shutdown() {
        // Perform an orderly shutdown of the executor service
        _executorService.shutdown();
    }

    // ----- PACKAGE PROTECTED METHODS ----- //

    void scheduleBlockRead(final FileJob fileJob) {
        Runnable r = new Runnable() {
            @Override
            public void run() {
                try {
                    fileJob.readOneBlock();
                } catch (Exception exc) {
                    fileJob.handleException(exc);
                }
            }
        };
        _executorService.execute(r);
    }

} // FileServer

class FileHandleImpl implements FileHandle {
    static private final Object LOCK = new Object();
    static private int _nextValue = 0;
    private final int _value;
    FileHandleImpl() {
        synchronized (FileHandleImpl.LOCK) {
            _value = FileHandleImpl._nextValue;
            FileHandleImpl._nextValue = FileHandleImpl._nextValue + 1;
        }
    }
    @Override
    public String toString() {
        return "FileHandleImpl(" + _value + ")";
    }
}

/**
 * TODO: Factor out BufferedReader, _currentIndex, and wrap-around logic into a DataRing
 *
 * File data is not ordered and may therefore be read as if it's a "ring" of data. A FileJob will continue to accept
 * observers and read in a circular fashion until all observers have read the file completely.
 */
class FileJob {

    /**
     * An ObserverEntry is complete when it has observed the entire ring of data.
     */
    static class ObserverEntry {

        private FileHandle _fileHandle;
        private int _firstIndex;
        private int _lastIndex;
        private FileObserver _observer;

        ObserverEntry(FileObserver observer, int firstIndex) {
            _fileHandle = new FileHandleImpl();
            _observer = observer;
            _firstIndex = firstIndex;
            _lastIndex = -1;
        }

        public FileHandle fileHandle() {
            return _fileHandle;
        }

        // ----- PACKAGE PROTECTED METHODS ----- //

        /**
         * This method is called each time a line of text is read. End-of-file is an edge case that calls this method
         * with a NULL line and -1 lineIndex. This allows the ObserverEntry to detect the completion of a ring when the
         * ring starts from the beginning of a file where the _lastIndex is -1 and the _firstIndex is 0.
         */
        void acceptLine(String line, int lineIndex) {
            if (line != null) {
                _observer.onNext(_fileHandle, line);
            }
            _lastIndex = lineIndex;
        }

        void close() {
            _observer.onClose(_fileHandle);
        }

        boolean isComplete() {
            boolean complete = _firstIndex == _lastIndex + 1;
            if (complete) {
                System.out.println("[" + _fileHandle + "] Circular read complete with first index " + _firstIndex +
                                   " and last index " + _lastIndex);
            }
            return complete;
        }

    } // ObserverEntry

    private BufferedReader _reader;
    private int _currentIndex;
    private List<ObserverEntry> _observerEntries;
    private String _path;

    FileJob(String path) {
        _path = path;
        _currentIndex = -1;
        _observerEntries = new ArrayList<ObserverEntry>(10);
        _reader = null;
    }

    synchronized public FileHandle openWithObserver(FileObserver observer) {
        int firstIndex = _currentIndex + 1;
        ObserverEntry oe = new ObserverEntry(observer, firstIndex);
        _observerEntries.add(oe);
        if (firstIndex != 0) {
            System.out.println("[" + oe.fileHandle() + "] Circular read started with first index: " + firstIndex);
        }
        if (_observerEntries.size() == 1) {
            // If we added the first entry, simulate a wrap-around to trigger a start
            handleEOF();
        }
        observer.onOpen(oe.fileHandle());
        return oe.fileHandle();
    }

    // ----- PACKAGE PROTECTED METHODS ----- //

    synchronized void handleEntryCompleted(ObserverEntry entry) {
        _observerEntries.remove(entry);
        entry.close();
    }

    synchronized void handleEOF() {
        _currentIndex = -1;
        if (_reader != null) {
            try {
                _reader.close();
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }
            _reader = null;
        }
        if (_observerEntries.size() > 0) {
            try {
                _reader = new BufferedReader(new FileReader(_path));
            } catch (Exception exc) {
                throw new RuntimeException(exc);
            }
            FileServiceImpl.singleton.scheduleBlockRead(this);
        }
    }

    synchronized void handleException(Exception exc) {
        exc.printStackTrace();
    }

    synchronized void readOneBlock()
        throws Exception
    {
        boolean eof = false;
        int readCount = 0;
        while (!eof && readCount < FileServiceImpl.BLOCK_SIZE) {
            String line = _reader.readLine();
            if (line == null) {
                eof = true;
                _currentIndex = -1;
            } else {
                readCount = readCount + 1;
                _currentIndex = _currentIndex + 1;
            }
            List<ObserverEntry> entries = new ArrayList<ObserverEntry>(_observerEntries);
            for (ObserverEntry oe: entries) {
                // If EOF, line will be null and _currentIndex will be -1
                oe.acceptLine(line, _currentIndex);
                if (oe.isComplete()) {
                    handleEntryCompleted(oe);
                }
            }
        }
        if (eof) {
            handleEOF();
        } else {
            if (_observerEntries.size() > 0) {
                // Fetch next block
                FileServiceImpl.singleton.scheduleBlockRead(this);
            } else {
                // All observers have been satisfied so close. This condition occurs when a ring of data
                // ends mid stream for the last observer, so we have to force a close through the EOF processing.
                handleEOF();
            }
        }
    }

} // FileJob
