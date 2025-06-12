use buildomat_github_database::types::{CheckRunVariety, JobFile};

pub fn disabled0() -> (String, String) {
    let path = ".github/buildomat/jobs/disabled0.sh";
    let content = "\
            #!/bin/bash\n\
            #:\n\
            #: name = \"disabled job\"\n\
            #: enable = false\n\
            #: variety = \"basic\"\n\
            #:\n\
            id -a\n\
            ";

    (path.to_string(), content.to_string())
}

pub fn fail0() -> (String, String, String) {
    let path = ".github/buildomat/jobs/case0.sh";
    let content = "\
            #!/bin/bash\n\
            ";

    (
        path.to_string(),
        content.to_string(),
        format!("TOML front matter in {path:?}: missing field `name`"),
    )
}

pub fn real0() -> (String, String, Option<JobFile>) {
    let path = ".github/buildomat/jobs/tuf-repo.sh";
    let content = "\
            #!/bin/bash\n\
            #:\n\
            #: name = \"helios / build TUF repo\"\n\
            #: variety = \"basic\"\n\
            #: target = \"helios-2.0\"\n\
            #: rust_toolchain = \"1.78.0\"\n\
            #: output_rules = [\n\
            #:	\"=/work/manifest.toml\",\n\
            #:	\"=/work/repo.zip\",\n\
            #:	\"=/work/repo.zip.sha256.txt\",\n\
            #:	\"%/work/*.log\",\n\
            #: ]\n\
            #: access_repos = [\n\
            #:	\"oxidecomputer/amd-apcb\",\n\
            #:	\"oxidecomputer/amd-efs\",\n\
            #:	\"oxidecomputer/amd-firmware\",\n\
            #:	\"oxidecomputer/amd-flash\",\n\
            #:	\"oxidecomputer/amd-host-image-builder\",\n\
            #:	\"oxidecomputer/boot-image-tools\",\n\
            #:	\"oxidecomputer/chelsio-t6-roms\",\n\
            #:	\"oxidecomputer/compliance-pilot\",\n\
            #:	\"oxidecomputer/facade\",\n\
            #:	\"oxidecomputer/helios\",\n\
            #:	\"oxidecomputer/helios-omicron-brand\",\n\
            #:	\"oxidecomputer/helios-omnios-build\",\n\
            #:	\"oxidecomputer/helios-omnios-extra\",\n\
            #:	\"oxidecomputer/bldb\",\n\
            #:	\"oxidecomputer/nanobl-rs\",\n\
            #: ]\n\
            #:\n\
            #: [[publish]]\n\
            #: series = \"rot-all\"\n\
            #: name = \"manifest.toml\"\n\
            #: from_output = \"/work/manifest.toml\"\n\
            #:\n\
            #: [[publish]]\n\
            #: series = \"rot-all\"\n\
            #: name = \"repo.zip\"\n\
            #: from_output = \"/work/repo.zip\"\n\
            #:\n\
            #: [[publish]]\n\
            #: series = \"rot-all\"\n\
            #: name = \"repo.zip.sha256.txt\"\n\
            #: from_output = \"/work/repo.zip.sha256.txt\"\n\
            #:\n\
            \n\
            set -o errexit\n\
            set -o pipefail\n\
            set -o xtrace\n\
            \n\
            cargo --version\n\
            rustc --version\n\
            \n\
            ptime -m ./tools/install_builder_prerequisites.sh -yp\n\
            source ./tools/include/force-git-over-https.sh\n\
            \n\
            rc=0\n\
            pfexec pkg install -q /system/zones/brand/omicron1/tools || rc=$?\n\
            case $rc in\n\
                # `man pkg` notes that exit code 4 means no changes were \
                made because\n\
                # there is nothing to do; that's fine. Any other exit code \
                is an error.\n\
                0 | 4) ;;\n\
                *) exit $rc ;;\n\
            esac\n\
            \n\
            pfexec zfs create -p \"rpool/images/$USER/host\"\n\
            pfexec zfs create -p \"rpool/images/$USER/recovery\"\n\
            \n\
            cargo xtask releng --output-dir /work\n\
        ";

    let expect = Some(JobFile {
        path: path.to_string(),
        name: "helios / build TUF repo".to_string(),
        variety: CheckRunVariety::Basic,
        config: serde_json::json!({
            "access_repos": [
                "oxidecomputer/amd-apcb",
                "oxidecomputer/amd-efs",
                "oxidecomputer/amd-firmware",
                "oxidecomputer/amd-flash",
                "oxidecomputer/amd-host-image-builder",
                "oxidecomputer/boot-image-tools",
                "oxidecomputer/chelsio-t6-roms",
                "oxidecomputer/compliance-pilot",
                "oxidecomputer/facade",
                "oxidecomputer/helios",
                "oxidecomputer/helios-omicron-brand",
                "oxidecomputer/helios-omnios-build",
                "oxidecomputer/helios-omnios-extra",
                "oxidecomputer/bldb",
                "oxidecomputer/nanobl-rs",
            ],
            "output_rules": [
                "=/work/manifest.toml",
                "=/work/repo.zip",
                "=/work/repo.zip.sha256.txt",
                "%/work/*.log",
            ],
            "publish": [
                {
                    "from_output": "/work/manifest.toml",
                    "name": "manifest.toml",
                    "series": "rot-all",
                },
                {
                    "from_output": "/work/repo.zip",
                    "name": "repo.zip",
                    "series": "rot-all",
                },
                {
                    "from_output": "/work/repo.zip.sha256.txt",
                    "name": "repo.zip.sha256.txt",
                    "series": "rot-all",
                },
            ],
            "rust_toolchain": "1.78.0",
            "target": "helios-2.0",
        }),
        content: content.to_string(),
        dependencies: Default::default(),
    });

    (path.to_string(), content.to_string(), expect)
}

#[cfg(test)]
pub mod test {
    use anyhow::{bail, Result};

    use super::*;

    #[test]
    fn jobfile_parse_basic() -> Result<()> {
        let (path, content, expect) = real0();

        let jf = JobFile::parse_content_at_path(&content, &path)?;

        if let Some((jf, expect)) = jf.as_ref().zip(expect.as_ref()) {
            if jf.config != expect.config {
                eprintln!(
                    "want config: {:#?}\ngot config: {:#?}",
                    expect.config, jf.config,
                );
            }
        }

        assert_eq!(expect, jf);
        Ok(())
    }

    #[test]
    fn jobfile_parse_failing() -> Result<()> {
        let (path, content, msg) = fail0();

        match JobFile::parse_content_at_path(&content, &path) {
            Ok(jf) => bail!("unexpected ok result: {jf:#?}"),
            Err(e) => {
                assert_eq!(e.to_string(), msg);
                Ok(())
            }
        }
    }

    #[test]
    fn jobfile_parse_disabled() {
        let (path, content) = disabled0();

        let jf = JobFile::parse_content_at_path(&content, &path);
        match jf {
            Ok(jf) => assert_eq!(jf, None),
            Err(e) => panic!("unexpected failure: {e}"),
        }
    }
}
