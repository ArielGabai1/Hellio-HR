"""Shared test data constants and runtime IDs."""

# Populated by setup_db fixture in conftest.py with actual UUIDs
IDS = {}

CANDIDATE_A = {
    "name": "Alex Mock",
    "status": "active",
    "experienceLevel": "senior",
    "contact": {
        "phone": "+1-555-000-0001",
        "email": "alex.mock@example.com",
        "location": "Mockville",
        "linkedin": "https://linkedin.com/in/alexmock",
        "github": "https://github.com/alexmock",
    },
    "languages": ["English", "Spanish"],
    "skills": ["AWS", "Kubernetes", "Terraform", "Docker", "Python"],
    "summary": "Senior DevOps engineer with experience in cloud infrastructure and container orchestration.",
    "experience": [
        {
            "title": "Senior DevOps Engineer",
            "company": "Acme Corp",
            "location": "Mockville",
            "startDate": "2021-01",
            "endDate": None,
            "bullets": [
                "Led migration of services to Kubernetes",
                "Implemented CI/CD pipelines using GitLab CI",
            ],
        },
        {
            "title": "DevOps Engineer",
            "company": "Widgets Inc",
            "location": "Testburg",
            "startDate": "2018-06",
            "endDate": "2020-12",
            "bullets": [
                "Managed AWS infrastructure with Terraform",
                "Built monitoring dashboards with Grafana",
            ],
        },
    ],
    "education": [
        {
            "degree": "B.Sc. Computer Science",
            "institution": "Mock University",
            "startDate": "2014",
            "endDate": "2018",
        }
    ],
    "certifications": [
        {"name": "AWS Solutions Architect", "year": 2022},
        {"name": "CKA", "year": 2021},
    ],
    "cvFile": "cv_001.pdf",
}

CANDIDATE_B = {
    "name": "Jordan Sample",
    "status": "inactive",
    "experienceLevel": "mid",
    "contact": {
        "phone": "+1-555-000-0002",
        "email": "jordan.sample@example.com",
        "location": "Testburg",
        "linkedin": "https://linkedin.com/in/jordansample",
        "github": "https://github.com/jordansample",
    },
    "languages": ["English"],
    "skills": ["Azure", "Docker", "Terraform", "Python", "Bash"],
    "summary": "Mid-level DevOps engineer with cloud automation experience.",
    "experience": [
        {
            "title": "DevOps Engineer",
            "company": "Sample Labs",
            "location": "Testburg",
            "startDate": "2022-03",
            "endDate": None,
            "bullets": [
                "Automated deployment processes with GitHub Actions",
                "Managed Azure infrastructure",
            ],
        }
    ],
    "education": [
        {
            "degree": "B.Sc. Software Engineering",
            "institution": "Test Institute",
            "startDate": "2017",
            "endDate": "2021",
        }
    ],
    "certifications": [],
    "cvFile": "cv_002.pdf",
}

POSITION_A = {
    "title": "Senior DevOps Engineer",
    "status": "open",
    "company": "Acme Corp",
    "hiringManager": {
        "name": "Pat Manager",
        "title": "VP Engineering",
        "email": "pat.manager@acme.example.com",
    },
    "experienceLevel": "senior",
    "requirements": [
        "5+ years DevOps experience",
        "AWS and Kubernetes in production",
        "Terraform proficiency",
    ],
    "niceToHave": [
        "Monitoring experience",
        "AWS certifications",
    ],
    "responsibilities": [
        "Lead infrastructure automation",
        "Mentor junior engineers",
    ],
    "techStack": ["AWS", "Kubernetes", "Terraform", "Python"],
    "location": "Mockville",
    "workArrangement": "Hybrid",
    "compensation": "Competitive",
    "timeline": "4 weeks",
    "summary": "Senior-level position focused on cloud infrastructure and automation at Acme Corp.",
    "jobFile": "job_001.txt",
}

POSITION_B = {
    "title": "Mid-Level Cloud Engineer",
    "status": "open",
    "company": "Widgets Inc",
    "hiringManager": {
        "name": "Sam Hirer",
        "title": "Engineering Manager",
        "email": "sam.hirer@widgets.example.com",
    },
    "experienceLevel": "mid",
    "requirements": [
        "2-5 years experience",
        "Cloud platform knowledge",
        "Python or Bash scripting",
    ],
    "niceToHave": [],
    "responsibilities": [
        "Manage cloud resources",
        "Write automation scripts",
    ],
    "techStack": ["Azure", "Docker", "Terraform", "Python"],
    "location": "Testburg",
    "workArrangement": "Remote",
    "compensation": "Entry level",
    "timeline": "2 weeks",
    "summary": "Entry-level cloud engineering role at Widgets Inc focused on automation.",
    "jobFile": "job_002.txt",
}
