#
# Copyright 2026 Oxide Computer Company
#

TOP =		$(PWD)

PUBLISHER =	buildomat

PROTO =		$(TOP)/proto
PKGDIR =	$(TOP)/packages
REPO =		$(PKGDIR)/repo

ARCHIVE =	$(PKGDIR)/buildomat-0.0.$(COMMIT_COUNT).p5p

PREFIX =	opt/buildomat
BINDIR =	$(PREFIX)/lib
SHAREDIR =	$(PREFIX)/share
BASICWWWDIR =	$(SHAREDIR)/variety/basic/www
WWWDIR =	$(SHAREDIR)/www
SMFDIR =	lib/svc/manifest/application/buildomat

DIRS_0 =	$(BINDIR) \
		$(BASICWWWDIR) \
		$(WWWDIR) \
		$(SMFDIR)
DIRS =		$(DIRS_0:%=$(PROTO)/%)

SMF_0 =		api.xml \
		github.xml \
		aws.xml
SMF =		$(SMF_0:%=$(PROTO)/$(SMFDIR)/%)

BINS_0 =	buildomat-server \
		buildomat-github-server \
		buildomat-github-ghtool \
		buildomat-github-dbtool \
		buildomat-factory-aws
BINS =		$(BINS_0:%=$(PROTO)/$(BINDIR)/%)

BASICWWW_0 =	live.js \
		style.css
WWW_0 =		bunyan.css \
		bunyan.js
SHAREFILES =	$(BASICWWW_0:%=$(PROTO)/$(BASICWWWDIR)/%) \
		$(WWW_0:%=$(PROTO)/$(WWWDIR)/%)

AGENTS_0 =	buildomat-agent \
		buildomat-agent-linux
AGENTS =	$(AGENTS_0:%=$(PROTO)/$(BINDIR)/%)
AGENTS_GZ =	$(AGENTS_0:%=$(PROTO)/$(BINDIR)/%.gz)

PACKAGES_0 =	buildomat
PACKAGES =	$(PACKAGES_0:%=pkg.%)

COMMIT_COUNT =	$(shell git rev-list --count HEAD)

.PHONY: all
all: $(DIRS) $(BINS) $(AGENTS) $(AGENTS_GZ) $(SMF) $(SHAREFILES)

.PHONY: package
package: all $(PKGDIR) $(PACKAGES)

.PHONY: pkg.%
pkg.%: $(PKGDIR)/%.final.p5m | $(REPO)
	pkgsend publish -d $(PROTO) -s $(REPO) $<

.PRECIOUS: $(PKGDIR)/%.base.p5m
$(PKGDIR)/%.base.p5m: %.p5m | $(PKGDIR)
	@rm -f $@
	sed -e 's/%PUBLISHER%/$(PUBLISHER)/g' \
	    -e 's/%COMMIT_COUNT%/$(COMMIT_COUNT)/g' \
	    $< | pkgmogrify -v -O $@

.PRECIOUS: $(PKGDIR)/%.generate.p5m
$(PKGDIR)/%.generate.p5m: $(PKGDIR)/%.base.p5m
	@rm -f $@
	pkgdepend generate -d $(PROTO) $< > $@

.PRECIOUS: $(PKGDIR)/%.resolve.p5m
$(PKGDIR)/%.generate.p5m.resolve.p5m: $(PKGDIR)/%.generate.p5m
	@rm -f $@
	pkgdepend resolve -d $(PKGDIR) -s resolve.p5m $<

.PRECIOUS: $(PKGDIR)/%.final.p5m
$(PKGDIR)/%.final.p5m: $(PKGDIR)/%.base.p5m $(PKGDIR)/%.generate.p5m.resolve.p5m
	@rm -f $@
	cat $^ > $@

$(REPO):
	pkgrepo create $@
	pkgrepo add-publisher -s $@ $(PUBLISHER)

.PHONY: archive
archive: $(ARCHIVE) | $(REPO)

.PHONY: $(ARCHIVE)
$(ARCHIVE):
	@rm -f $@
	pkgrecv -a -d $@ -s $(REPO) -v -m latest '*'

$(PROTO)/$(SMFDIR)/%: smf/%
	@rm -f $@
	cp $< $@

$(DIRS) $(PKGDIR):
	mkdir -p $@

.PHONY: $(BINS)
$(BINS): | $(DIRS)
	@rm -f $@
	cp ../target/release/$(@F) $@

$(PROTO)/$(BINDIR)/buildomat-agent.gz: $(INPUT_AGENT_ILLUMOS_GZ) | $(DIRS)
	@rm -f $@
	test -n "$(INPUT_AGENT_ILLUMOS_GZ)" || \
	    (echo "set INPUT_AGENT_ILLUMOS_GZ?"; exit 1)
	cp $(INPUT_AGENT_ILLUMOS_GZ) $@

$(PROTO)/$(BINDIR)/buildomat-agent-linux.gz: $(INPUT_AGENT_LINUX_GZ) | $(DIRS)
	@rm -f $@
	test -n "$(INPUT_AGENT_LINUX_GZ)" || \
	    (echo "set INPUT_AGENT_LINUX_GZ?"; exit 1)
	cp $(INPUT_AGENT_LINUX_GZ) $@

$(PROTO)/$(BINDIR)/%: $(PROTO)/$(BINDIR)/%.gz | $(DIRS)
	@rm -f $@
	gunzip < $< > $@

$(PROTO)/$(WWWDIR)/%: ../github/server/www/% | $(DIRS)
	@rm -f $@
	cp $< $@

$(PROTO)/$(SHAREDIR)/variety/%: ../variety/% | $(DIRS)
	@rm -f $@
	cp $< $@

.PHONY: pkgfmt
pkgfmt: $(PACKAGES_0:%=pkgfmt.%)
	@echo ok

.PHONY: pkgfmt.%
pkgfmt.%:
	pkgfmt -f v2 $(@:pkgfmt.%=%.p5m)

.PHONY: check-xml
check-xml: $(SMF_0:%=check-xml.%)

check-xml.%:
	xmllint --dtdvalid /usr/share/lib/xml/dtd/service_bundle.dtd.1 \
	    $(@:check-xml.%=smf/%) >/dev/null

.PHONY: check-pkg
check-pkg: $(PACKAGES_0:%=check-pkg.%)

.PHONY: check-pkg.%
check-pkg.%:
	pkgfmt -c -f v2 $(@:check-pkg.%=%.p5m)

.PHONY: check
check: check-xml check-pkg

.PHONY: clean
clean:
	rm -rf $(PROTO)

.PHONY: clobber
clobber: clean
	rm -rf $(PKGDIR)
