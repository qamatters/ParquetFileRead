import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.page.PageReadStore;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.convert.GroupRecordConverter;
import org.apache.parquet.format.converter.ParquetMetadataConverter;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.io.ColumnIOFactory;
import org.apache.parquet.io.MessageColumnIO;
import org.apache.parquet.io.RecordReader;
import org.apache.parquet.schema.MessageType;
import org.testng.annotations.Test;

import java.io.IOException;

import static com.ReadParaquetFile.printGroup;

public class ReadParquetTest {
    private static Path path = new Path("data//userdata1.parquet");
    @Test (enabled = false)
    public static void read() throws IllegalArgumentException {
        Configuration conf = new Configuration();
        try {
            ParquetMetadata readFooter = ParquetFileReader.readFooter(conf, path, ParquetMetadataConverter.NO_FILTER);
            MessageType schema = readFooter.getFileMetaData().getSchema();
            ParquetFileReader r = new ParquetFileReader(conf, path, readFooter);
            PageReadStore pages = null;
            try {
                while (null != (pages = r.readNextRowGroup())) {
                    final long rows = pages.getRowCount();
                    System.out.println("Number of rows: " + rows);
                    final MessageColumnIO columnIO = new ColumnIOFactory().getColumnIO(schema);
                    final RecordReader<Group> recordReader = columnIO.getRecordReader(pages, new GroupRecordConverter(schema));

                    for (int i = 0; i < rows; i++) {
                        final Object g = recordReader.read();
                        printGroup((Group) g);
                    }
                }
            } finally {
                r.close();
            }
        } catch (IOException e) {
            System.out.println("Error reading parquet file.");
            e.printStackTrace();
        }
    }
}
