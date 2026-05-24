FROM debian:bookworm-slim

WORKDIR /workdir

# TeX Live deps (small ver)
# --no-install-recommends giúp giảm dung lượng tối đa
RUN apt-get update && apt-get install -y --no-install-recommends \
    texlive-latex-recommended \
    texlive-fonts-recommended \
    texlive-lang-other \
    texlive-latex-extra \
    ghostscript \
    && rm -rf /var/lib/apt/lists/*

# Install vietnamese fonts
RUN apt-get update && apt-get install -y fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

# Install biblatex
RUN apt-get update && apt-get install -y texlive-bibtex-extra \
    && rm -rf /var/lib/apt/lists/*

# Install textlive-science
RUN apt-get update && apt-get install -y texlive-science \
    && rm -rf /var/lib/apt/lists/*

# Install biber
RUN apt-get update && apt-get install -y biber \
    && rm -rf /var/lib/apt/lists/*

# Install texlive-fonts-extra
RUN apt-get update && apt-get install -y texlive-fonts-extra \
    && rm -rf /var/lib/apt/lists/*

# Re-install textlive-science
RUN apt-get update && apt-get install -y texlive-plain-generic\
    && rm -rf /var/lib/apt/lists/*

RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    unzip \
    && rm -rf /var/lib/apt/lists/*

RUN wget http://mirrors.ctan.org/fonts/mtp2lite.zip -O /tmp/mtp2lite.zip \
    && unzip /tmp/mtp2lite.zip -d /tmp \
    && TEXMF=$(kpsewhich -var-value TEXMFLOCAL) \
    && cp -r /tmp/mtp2lite/texmf/* $TEXMF/ \
    && texhash \
    && updmap-sys --enable Map=mtpro2.map \
    && rm -rf /tmp/mtp2lite.zip /tmp/mtp2lite

RUN apt-get update && apt-get install -y --no-install-recommends \
    tex-gyre \
    && rm -rf /var/lib/apt/lists/*

# Default cmd
CMD ["pdflatex"]
