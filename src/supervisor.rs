use std::{
    env,
    ffi::CString,
    process::{self, Stdio},
};

use tokio::{
    process::Command,
    time::{Duration, timeout},
};

use cgroups_rs::{cgroup_builder::CgroupBuilder, hierarchies, CgroupPid};

use crate::jobs_management::JobConfiguration;

use libc::chown;

pub async fn supervisor(task_id: &str, data: &str) {
    println!("Parsing job configuration");
    let job_configuration: JobConfiguration = serde_json::from_str(&data).unwrap();
    println!("Create cgroup");
    let hier = hierarchies::auto();
    let cgroup = CgroupBuilder::new(&task_id)
        .cpu()
        .cpus(job_configuration.requirement.cpus.to_string().unwrap())
        .mems(job_configuration.requirement.mems.to_string().unwrap())
        .done()
        .memory()
        .memory_hard_limit(job_configuration.requirement.countables.get("memory") as i64)
        .done()
        .build(hier)
        .unwrap();
    println!("Get into cgroup");
    cgroup
        .add_task_by_tgid(CgroupPid::from(process::id() as u64))
        .unwrap();
    println!("Create log files");
    let stdout = std::fs::File::open(&job_configuration.stdout_file).unwrap();
    let stderr = std::fs::File::open(&job_configuration.stderr_file).unwrap();
    unsafe {
        let stdout = CString::new(job_configuration.stdout_file.as_str()).unwrap();
        let stderr = CString::new(job_configuration.stderr_file.as_str()).unwrap();
        if chown(
            stdout.as_ptr(),
            job_configuration.uid,
            job_configuration.gid,
        ) != 0
            || chown(
                stderr.as_ptr(),
                job_configuration.uid,
                job_configuration.gid,
            ) != 0
        {
            panic!("Failed to set privilleges on log files")
        }
    }
    println!("Start executor");
    let program = env::current_exe().unwrap();
    let mut child = Command::new(program)
        .arg("executor")
        .arg(data)
        .uid(job_configuration.uid)
        .gid(job_configuration.gid)
        .stdout(Stdio::from(stdout))
        .stderr(Stdio::from(stderr))
        .spawn()
        .unwrap();

    let exit_status = child.wait();
    let time_limit = timeout(Duration::from_secs(job_configuration.requirement.countables.get("time_limit") as u64), exit_status).await;
    if let Ok(exit_status) = time_limit {
        println!("Executor exited. \n{:#?}", exit_status.unwrap());
    } else {
        child.kill().await.unwrap();
        println!("Time limit reached!");
    }
    
    println!("Clean cgroup");
    cgroup
        .remove_task_by_tgid(CgroupPid::from(process::id() as u64))
        .unwrap();
    cgroup.kill().unwrap();
    cgroup.delete().unwrap();
    println!("Cgroup cleaned, exit.")
}
