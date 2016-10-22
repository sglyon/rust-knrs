use std::time::SystemTime;
use std::path::PathBuf;

extern crate quick_csv;
extern crate csv;
extern crate glob;
extern crate pretty_bytes;

extern crate scoped_threadpool;
use scoped_threadpool::Pool;

pub type InputFile = quick_csv::Csv<std::io::BufReader<std::fs::File>>;
pub type OutputFile = csv::Writer<std::fs::File>;

pub trait KRNSMovementOperator {
    type T: Sync;

    // Required to be implemented by all implementing structs
    fn summarize_one_file(&self, input: &mut InputFile, output: &mut OutputFile);
    fn create_out(&self) -> Self::T;

    // functions with default implementations
    #[allow(unused_variables)]
    fn aggregate_one_summary(&self, input: &mut InputFile, out: &mut Self::T) {}

    #[allow(unused_variables)]
    fn write_aggregate(&self, writer: &mut OutputFile, out: &Self::T) {}

    fn summary_dir(&self) -> PathBuf {
        let out = std::env::current_dir().unwrap();
        out.join("summaries")
    }

    fn summary_extension(&self) -> &str {
        "tsv"
    }

    fn summary_glob_pattern(&self) -> String {
        let mut out = self.summary_dir();
        out.push("*");
        out.set_extension(self.summary_extension());
        let pat = &out.to_str().expect("Couldn't construct summary globber");
        pat.to_string()
    }

    fn summary_files(&self) -> Vec<PathBuf> {
        self.glob_to_pathbufs(&self.summary_glob_pattern())
    }

    fn glob_to_pathbufs(&self, pat: &str) -> Vec<PathBuf> {
        let mut out: Vec<_> = vec![];
        let globbed = glob::glob(pat).unwrap();
        for entry in globbed {
            match entry {
                Ok(x) => {
                    if x != self.aggregate_filename() {
                        out.push(x);
                    }
                }
                _ => panic!("funky globber"),
            }
        }
        out
    }

    fn aggregate_basename(&self) -> &str {
        "aggregated"
    }

    fn aggregate_filename(&self) -> PathBuf {
        let mut out = self.summary_dir();
        out.push(self.aggregate_basename());
        out.set_extension(self.summary_extension());
        out
    }

    fn summarize_threads(&self) -> Option<u32> {
        None
    }

    // Return path to sumary file, given current file.
    fn summary_path(&self, p: &PathBuf) -> PathBuf {
        let mut dir = self.summary_dir();
        let p_name = p.file_name().expect("need a file name");
        dir.push(p_name);
        dir.set_extension(self.summary_extension());
        dir
    }


    fn aggregate_summaries(&self) {
        let summary_files = self.summary_files();
        let mut out = self.create_out();
        for p in &summary_files {
            println!("  [knrs aggregate] input:\t{}", p.display());
            let mut input = quick_csv::Csv::from_file(p)
                .expect("Couldn't find input file")
                .delimiter(b'\t')
                .has_header(true);
            self.aggregate_one_summary(&mut input, &mut out);
        }

        let out_fn = self.aggregate_filename();
        println!("  [knrs aggregate] output:\t{}", out_fn.display());

        let mut writer = csv::Writer::from_file(out_fn)
            .expect("couldn't create csv for writing")
            .delimiter(b'\t');

        self.write_aggregate(&mut writer, &mut out);
    }

    fn summarize(&self, paths: &[PathBuf])
        where Self: Sync
    {
        let start_time = SystemTime::now();
        if let Some(threads) = self.summarize_threads() {
            let mut pool = Pool::new(threads);
            pool.scoped(|scoped| {
                for p in paths {
                    let p2 = self.summary_path(&p);
                    let (mut input, mut output) = pre_summarize(p, &p2);

                    scoped.execute(move || {
                        self.summarize_one_file(&mut input, &mut output);
                    });
                }
            });
        } else {
            for p in paths {
                let p2 = self.summary_path(&p);
                let (mut input, mut output) = pre_summarize(p, &p2);
                self.summarize_one_file(&mut input, &mut output);
            }
        }
        println!(" [knrs] Total time {:?}", start_time.elapsed().unwrap());
    }
}

fn pre_summarize(p: &PathBuf, p2: &PathBuf) -> (InputFile, OutputFile) {
    let p_bytes = std::fs::metadata(&p).unwrap().len();
    println!("  [knrs] input:\t{} ({})\n  [knrs] output:\t{}",
             p.display(),
             pretty_bytes::converter::convert(p_bytes as f64),
             p2.display());

    let p2_par = p2.parent().expect("p2 should have a parent");
    if !p2_par.is_dir() {
        if p2_par.exists() {
            panic!("Output path exists, but isn't directory");
        } else {
            println!("  [knrs] creating output directory:\t{}", p2_par.display());
            std::fs::create_dir_all(p2_par).unwrap_or_else(|why| {
                println!("! {:?}", why.kind());
            });
        }
    }

    let input = quick_csv::Csv::from_file(p)
        .expect("Couldn't find input file")
        .delimiter(b'\t')
        .has_header(true);

    let output = csv::Writer::from_file(&p2)
        .expect("couldn't create csv for writing")
        .delimiter(b'\t');

    return (input, output);
}
