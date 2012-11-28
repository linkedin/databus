# set latexfile to the name of the main file without the .tex
latexfile = databus
# put the names of figure files here.  include the .eps
#figures = smiley.eps logo.eps

# if no references, run "touch latexfile.bbl; touch referencefile.bib"
# or delete dependency in .dvi and tarball sections
referencefile = refs

TEX = latex
BIBTEX = bibtex

# support subdirectories
VPATH = figures

# reruns latex if needed.  to get rid of this capability, delete the
# three lines after the rule.  Delete .bbl dependency if not using
# BibTeX references.
# idea from http://ctan.unsw.edu.au/help/uk-tex-faq/Makefile
#$(latexfile).dvi : $(latexfile).tex $(latexfile).bbl
$(latexfile).dvi : *.tex $(VPATH)/*.eps
	while ($(TEX) $(latexfile) ; \
	grep -q "Rerun to get cross" $(latexfile).log ) do true ; \
	done

$(latexfile).bbl : 
	$(BIBTEX) $(latexfile)

# keep .eps files in the same directory as the .fig files
#%.eps : %.fig
#  fig2dev -L eps $< > $(dir $< )$@

$(latexfile).pdf : $(latexfile).ps
	ps2pdf $(latexfile).ps $(latexfile).pdf

pdf : $(latexfile).pdf

$(latexfile).ps : $(latexfile).dvi
	dvips $(latexfile)

ps : $(latexfile).ps 

# make can't know all the sourcefiles.  some file may not have
# sourcefiles, e.g. eps's taken from other documents. 
$(latexfile).tar.gz : $(latexfile).tex $(referencefile).bib
	tar -czvf $(latexfile).tar.gz $^

tarball: $(latexfile).tar.gz

