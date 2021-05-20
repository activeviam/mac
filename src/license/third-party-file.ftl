<#--
  #%L
  License Maven Plugin
  %%
  Copyright (C) 2012 Codehaus, Tony Chemit
  %%
  This program is free software: you can redistribute it and/or modify
  it under the terms of the GNU Lesser General Public License as
  published by the Free Software Foundation, either version 3 of the
  License, or (at your option) any later version.
  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Lesser Public License for more details.
  You should have received a copy of the GNU General Lesser Public
  License along with this program.  If not, see
  <http://www.gnu.org/licenses/lgpl-3.0.html>.
  #L%
  -->
<#-- To render the third-party file.
 Available context :
 - dependencyMap a collection of Map.Entry with
   key are dependencies (as a MavenProject) (from the maven project)
   values are licenses of each dependency (array of string)
 - licenseMap a collection of Map.Entry with
   key are licenses of each dependency (array of string)
   values are all dependencies using this license
-->
<#--
 Functions
-->
<#function licenseFormat licenses>
    <#assign result = ""/>
    <#list licenses as license>
		<#if result?length &gt; 0>
			<#assign result = result + ";" + license/>
		<#else>
			<#assign result = license/>
		</#if>
    </#list>
    <#return result>
</#function>
<#function artifactName p>
    <#if p.name?index_of('Unnamed') &gt; -1>
        <#return p.artifactId>
    <#else>
        <#return p.name>
    </#if>
</#function>
<#function artifactFormat p>
    <#return p.groupId + ":" + p.artifactId + "\t" + (p.url!"") + "\t" + p.version>
</#function>
<#--
 Output
-->
<#if dependencyMap?size == 0>
The project has no dependencies.
<#else>
Name	Artifact	Url	Version	Licenses
    <#list dependencyMap as e>
        <#assign project = e.getKey()/>
        <#assign licenses = e.getValue()/>
${artifactName(project)}	${artifactFormat(project)}	${licenseFormat(licenses)}
    </#list>
</#if>
