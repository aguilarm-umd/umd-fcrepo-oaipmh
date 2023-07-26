<xsl:stylesheet version="1.0" xmlns:xsl="http://www.w3.org/1999/XSL/Transform"
                xmlns:dc="http://purl.org/dc/elements/1.1/"
                xmlns:dcterms="http://purl.org/dc/terms/"
                xmlns:edm="http://www.europeana.eu/schemas/edm/"
                xmlns:oai_dc="http://www.openarchives.org/OAI/2.0/oai_dc/"
                xmlns:owl="http://www.w3.org/2002/07/owl#"
                xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
                xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
                xmlns:rel="http://id.loc.gov/vocabulary/relators/"
                xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
  <xsl:strip-space elements="*"/>
  <xsl:param name="handle_proxy_prefix" />

  <xsl:template match="text()"/>

  <xsl:template match="/">
    <oai_dc:dc xsi:schemaLocation="http://www.openarchives.org/OAI/2.0/oai_dc/ http://www.openarchives.org/OAI/2.0/oai_dc.xsd">
      <!-- select the top-level object -->
      <xsl:apply-templates select="rdf:RDF/rdf:Description[rdf:type/@rdf:resource = 'http://pcdm.org/models#Object']"/>
    </oai_dc:dc>
  </xsl:template>

  <!-- contributor -->
  <xsl:template match="dc:contributor|dcterms:contributor">
    <dc:contributor><xsl:call-template name="embedded-resource-or-literal"/></dc:contributor>
  </xsl:template>

  <!-- coverage -->
  <xsl:template match="dcterms:spatial">
    <dc:coverage><xsl:call-template name="embedded-resource-or-literal"/></dc:coverage>
  </xsl:template>

  <!-- creator -->
  <xsl:template match="dc:creator|dcterms:creator|rel:aut">
    <dc:creator><xsl:call-template name="embedded-resource-or-literal"/></dc:creator>
  </xsl:template>

  <!-- date -->
  <xsl:template match="dc:date">
    <dc:date><xsl:value-of select="."/></dc:date>
  </xsl:template>

  <!-- description -->
  <xsl:template match="dcterms:description">
    <dc:description><xsl:value-of select="."/></dc:description>
  </xsl:template>

  <!-- format -->
  <xsl:template match="dcterms:extent">
    <dc:format><xsl:value-of select="."/></dc:format>
  </xsl:template>

  <!-- identifier -->
  <xsl:template match="dcterms:identifier">
  <dc:identifier><xsl:value-of select="."/></dc:identifier>
  <xsl:if test="starts-with(., 'hdl:')">
    <dc:identifier><xsl:value-of select="$handle_proxy_prefix"/><xsl:value-of select="substring(., 5)"/></dc:identifier>
  </xsl:if>
</xsl:template>

  <!-- language -->
  <xsl:template match="dc:language">
    <dc:language><xsl:value-of select="."/></dc:language>
  </xsl:template>

  <!-- publisher -->

  <!-- relation -->
  <xsl:template match="dcterms:isPartOf">
    <dc:relation>
      <xsl:variable name="target-uri" select="@rdf:resource"/>
      <xsl:value-of select="//rdf:Description[@rdf:about=$target-uri]/owl:sameAs/@rdf:resource"/>
    </dc:relation>
  </xsl:template>

  <!-- rights -->
  <xsl:template match="dcterms:rights">
    <dc:rights><xsl:call-template name="resource-or-literal"/></dc:rights>
  </xsl:template>

  <!-- source -->

  <!-- subject -->
  <xsl:template match="dcterms:subject">
    <dc:subject>
      <xsl:variable name="target-uri" select="@rdf:resource"/>
      <xsl:value-of select="//rdf:Description[@rdf:about=$target-uri]/rdfs:label"/>
    </dc:subject>
  </xsl:template>

  <!-- title -->
  <xsl:template match="dcterms:title">
    <dc:title><xsl:value-of select="."/></dc:title>
  </xsl:template>

  <!-- type  -->
  <xsl:template match="dcterms:type|edm:hasType">
    <dc:type><xsl:value-of select="@rdf:resource"/></dc:type>
  </xsl:template>


  <!-- UTILITY TEMPLATES -->

  <xsl:template name="resource-or-literal">
    <xsl:choose>
      <xsl:when test="@rdf:resource">
        <xsl:value-of select="@rdf:resource"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="."/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

  <xsl:template name="embedded-resource-or-literal">
    <xsl:choose>
      <xsl:when test="@rdf:resource">
        <xsl:variable name="target-uri" select="@rdf:resource"/>
        <xsl:value-of select="//rdf:Description[@rdf:about=$target-uri]/rdfs:label"/>
      </xsl:when>
      <xsl:otherwise>
        <xsl:value-of select="."/>
      </xsl:otherwise>
    </xsl:choose>
  </xsl:template>

</xsl:stylesheet>
